// test/clientv3-compat/main.go — clientv3 compatibility smoke for crab-watchstore (cw-u4a.24).
//
// Uses the REAL go.etcd.io/etcd/client/v3 library (v3.5.x) to exercise
// KV / Txn / Watch / Lease / Compact against a running crab-watchstore
// and asserts correct behaviour at each step.  Exit non-zero on any failure.
//
// Usage:  go run . 127.0.0.1:PORT
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	passed int
	failed int
)

func ok(label string)          { passed++; fmt.Printf("  ok   %s\n", label) }
func bad(label, reason string) { failed++; fmt.Printf("  FAIL %s: %s\n", label, reason) }
func mustOK(label string, err error) bool {
	if err != nil {
		bad(label, err.Error())
		return false
	}
	ok(label)
	return true
}

// isCompactedErr returns true when gRPC status is OUT_OF_RANGE (ErrCompacted).
func isCompactedErr(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if ok && st.Code() == codes.OutOfRange {
		return true
	}
	return strings.Contains(err.Error(), "compacted")
}

// ============================================================
// KV section
// ============================================================

func testKV(cli *clientv3.Client) {
	fmt.Println("\n== KV ==")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ---- Put / Get basic ----
	_, err := cli.Put(ctx, "kv/foo", "bar")
	mustOK("Put kv/foo=bar", err)

	resp, err := cli.Get(ctx, "kv/foo")
	if mustOK("Get kv/foo -> bar", err) {
		if len(resp.Kvs) == 1 && string(resp.Kvs[0].Value) == "bar" {
			ok("Get kv/foo value correct")
		} else {
			bad("Get kv/foo value correct", fmt.Sprintf("kvs=%v", resp.Kvs))
		}
	}

	// ---- Revision fields after overwrite ----
	_, err = cli.Put(ctx, "kv/foo", "baz")
	mustOK("Put kv/foo=baz (overwrite)", err)

	resp, err = cli.Get(ctx, "kv/foo")
	if mustOK("Get kv/foo -> baz", err) {
		kv := resp.Kvs[0]
		if string(kv.Value) == "baz" {
			ok("Get kv/foo value = baz after overwrite")
		} else {
			bad("Get kv/foo value = baz", fmt.Sprintf("got %q", string(kv.Value)))
		}
		if kv.Version == 2 {
			ok("kv/foo version=2 after overwrite")
		} else {
			bad("kv/foo version=2", fmt.Sprintf("got %d", kv.Version))
		}
		if kv.ModRevision > kv.CreateRevision {
			ok("mod_revision > create_revision after overwrite")
		} else {
			bad("mod_revision > create_revision", fmt.Sprintf("create=%d mod=%d", kv.CreateRevision, kv.ModRevision))
		}
	}

	// ---- WithRev: historical read returns old value ----
	// Get the create_revision from the first write (version=1 means CreateRev == put rev)
	resp, _ = cli.Get(ctx, "kv/foo")
	createRev := resp.Kvs[0].CreateRevision
	histResp, err := cli.Get(ctx, "kv/foo", clientv3.WithRev(createRev))
	if mustOK("Get kv/foo WithRev(createRev) succeeds", err) {
		if len(histResp.Kvs) == 1 && string(histResp.Kvs[0].Value) == "bar" {
			ok("Get kv/foo WithRev(createRev) returns original value bar")
		} else {
			bad("Get kv/foo WithRev(createRev) returns bar",
				fmt.Sprintf("kvs=%v", histResp.Kvs))
		}
	}

	// ---- WithPrefix ----
	cli.Put(ctx, "kv/a", "1")
	cli.Put(ctx, "kv/b", "2")
	cli.Put(ctx, "kv/c", "3")

	pfxResp, err := cli.Get(ctx, "kv/", clientv3.WithPrefix())
	if mustOK("Get kv/ WithPrefix succeeds", err) {
		if len(pfxResp.Kvs) >= 4 {
			ok(fmt.Sprintf("WithPrefix returned %d keys (>=4)", len(pfxResp.Kvs)))
		} else {
			bad("WithPrefix returned >=4 keys", fmt.Sprintf("got %d", len(pfxResp.Kvs)))
		}
	}

	// ---- WithLimit ----
	limResp, err := cli.Get(ctx, "kv/", clientv3.WithPrefix(), clientv3.WithLimit(2))
	if mustOK("Get kv/ WithPrefix WithLimit(2) succeeds", err) {
		if len(limResp.Kvs) == 2 {
			ok("WithLimit(2) returned exactly 2 keys")
		} else {
			bad("WithLimit(2) returned 2 keys", fmt.Sprintf("got %d", len(limResp.Kvs)))
		}
		if limResp.More {
			ok("WithLimit(2) sets more=true")
		} else {
			bad("WithLimit(2) sets more=true", "more was false")
		}
	}

	// ---- WithRange (lexicographic range, exclusive end) ----
	cli.Put(ctx, "rng/a", "ra")
	cli.Put(ctx, "rng/b", "rb")
	cli.Put(ctx, "rng/c", "rc")
	rngResp, err := cli.Get(ctx, "rng/a", clientv3.WithRange("rng/c"))
	if mustOK("Get rng/a..rng/c WithRange succeeds", err) {
		if len(rngResp.Kvs) == 2 {
			ok("WithRange [rng/a, rng/c) returned 2 keys (rng/a + rng/b)")
		} else {
			bad("WithRange returned 2 keys", fmt.Sprintf("got %d", len(rngResp.Kvs)))
		}
	}

	// ---- WithKeysOnly ----
	koResp, err := cli.Get(ctx, "kv/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if mustOK("Get kv/ WithKeysOnly succeeds", err) {
		allBlank := true
		for _, kv := range koResp.Kvs {
			if len(kv.Value) != 0 {
				allBlank = false
			}
		}
		if allBlank {
			ok("WithKeysOnly returned empty values")
		} else {
			bad("WithKeysOnly returned empty values", "some values were non-empty")
		}
	}

	// ---- WithCountOnly ----
	coResp, err := cli.Get(ctx, "kv/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if mustOK("Get kv/ WithCountOnly succeeds", err) {
		if coResp.Count >= 4 {
			ok(fmt.Sprintf("WithCountOnly Count=%d (>=4)", coResp.Count))
		} else {
			bad("WithCountOnly Count>=4", fmt.Sprintf("got Count=%d", coResp.Count))
		}
		if len(coResp.Kvs) == 0 {
			ok("WithCountOnly Kvs is empty (no data returned)")
		} else {
			bad("WithCountOnly Kvs empty", fmt.Sprintf("got %d kvs", len(coResp.Kvs)))
		}
	}

	// ---- WithSort (descending by key) ----
	sortResp, err := cli.Get(ctx, "kv/", clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if mustOK("Get kv/ WithSort(SortByKey, SortDescend) succeeds", err) {
		sorted := true
		for i := 1; i < len(sortResp.Kvs); i++ {
			if string(sortResp.Kvs[i-1].Key) < string(sortResp.Kvs[i].Key) {
				sorted = false
			}
		}
		if sorted {
			ok("WithSort(SortByKey, SortDescend) keys are in descending order")
		} else {
			keys := make([]string, len(sortResp.Kvs))
			for i, kv := range sortResp.Kvs {
				keys[i] = string(kv.Key)
			}
			bad("WithSort descending keys", fmt.Sprintf("order: %v", keys))
		}
	}

	// ---- WithPrevKV on Put ----
	cli.Put(ctx, "kv/prev", "first")
	prevPutResp, err := cli.Put(ctx, "kv/prev", "second", clientv3.WithPrevKV())
	if mustOK("Put kv/prev=second WithPrevKV succeeds", err) {
		if prevPutResp.PrevKv != nil && string(prevPutResp.PrevKv.Value) == "first" {
			ok("Put WithPrevKV returned prev value = first")
		} else {
			pv := ""
			if prevPutResp.PrevKv != nil {
				pv = string(prevPutResp.PrevKv.Value)
			}
			bad("Put WithPrevKV prev value = first", fmt.Sprintf("got %q (PrevKv=%v)", pv, prevPutResp.PrevKv))
		}
	}

	// ---- WithPrevKV on Delete ----
	prevDelResp, err := cli.Delete(ctx, "kv/prev", clientv3.WithPrevKV())
	if mustOK("Delete kv/prev WithPrevKV succeeds", err) {
		if len(prevDelResp.PrevKvs) == 1 && string(prevDelResp.PrevKvs[0].Value) == "second" {
			ok("Delete WithPrevKV returned prev value = second")
		} else {
			bad("Delete WithPrevKV prev value = second", fmt.Sprintf("PrevKvs=%v", prevDelResp.PrevKvs))
		}
	}

	// ---- Delete with WithPrefix ----
	delPfxResp, err := cli.Delete(ctx, "kv/", clientv3.WithPrefix())
	if mustOK("Delete kv/ WithPrefix succeeds", err) {
		if delPfxResp.Deleted >= 4 {
			ok(fmt.Sprintf("Delete WithPrefix deleted %d keys (>=4)", delPfxResp.Deleted))
		} else {
			bad("Delete WithPrefix deleted >=4 keys", fmt.Sprintf("got %d", delPfxResp.Deleted))
		}
	}

	// Confirm the prefix is now empty
	emptyResp, err := cli.Get(ctx, "kv/", clientv3.WithPrefix())
	if mustOK("Get kv/ after bulk delete succeeds", err) {
		if len(emptyResp.Kvs) == 0 {
			ok("Get kv/ after bulk delete is empty")
		} else {
			bad("Get kv/ after bulk delete is empty", fmt.Sprintf("got %d kvs", len(emptyResp.Kvs)))
		}
	}
}

// ============================================================
// Txn section
// ============================================================

func testTxn(cli *clientv3.Client) {
	fmt.Println("\n== TXN ==")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cli.Put(ctx, "txn/tk", "hello")

	// SUCCESS branch: value("txn/tk") == "hello" -> put txn/tk=world
	succResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value("txn/tk"), "=", "hello")).
		Then(clientv3.OpPut("txn/tk", "world")).
		Else(clientv3.OpGet("txn/tk")).
		Commit()
	if mustOK("Txn success branch commit succeeds", err) {
		if succResp.Succeeded {
			ok("Txn success branch: Succeeded=true")
		} else {
			bad("Txn success branch Succeeded=true", "Succeeded=false")
		}
	}

	tkResp, _ := cli.Get(ctx, "txn/tk")
	if len(tkResp.Kvs) == 1 && string(tkResp.Kvs[0].Value) == "world" {
		ok("Txn success branch applied: txn/tk=world")
	} else {
		v := ""
		if len(tkResp.Kvs) == 1 {
			v = string(tkResp.Kvs[0].Value)
		}
		bad("Txn success applied txn/tk=world", fmt.Sprintf("got %q", v))
	}

	// FAILURE branch: value("txn/tk") == "nope" -> put txn/tk=BAD (compare false)
	failResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value("txn/tk"), "=", "nope")).
		Then(clientv3.OpPut("txn/tk", "BAD")).
		Else(clientv3.OpGet("txn/tk")).
		Commit()
	if mustOK("Txn failure branch commit succeeds", err) {
		if !failResp.Succeeded {
			ok("Txn failure branch: Succeeded=false")
		} else {
			bad("Txn failure branch Succeeded=false", "Succeeded=true")
		}
	}

	tkResp2, _ := cli.Get(ctx, "txn/tk")
	if len(tkResp2.Kvs) == 1 && string(tkResp2.Kvs[0].Value) == "world" {
		ok("Txn failure branch did NOT overwrite txn/tk (still world)")
	} else {
		v := ""
		if len(tkResp2.Kvs) == 1 {
			v = string(tkResp2.Kvs[0].Value)
		}
		bad("Txn failure branch preserved txn/tk=world", fmt.Sprintf("got %q", v))
	}

	// Txn with multiple ops in success branch
	multiResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Value("txn/tk"), "=", "world")).
		Then(
			clientv3.OpPut("txn/m1", "mv1"),
			clientv3.OpPut("txn/m2", "mv2"),
		).
		Commit()
	if mustOK("Txn multi-op success branch commit succeeds", err) {
		if multiResp.Succeeded {
			ok("Txn multi-op Succeeded=true")
		} else {
			bad("Txn multi-op Succeeded=true", "Succeeded=false")
		}
	}
	m1, _ := cli.Get(ctx, "txn/m1")
	m2, _ := cli.Get(ctx, "txn/m2")
	if len(m1.Kvs) == 1 && string(m1.Kvs[0].Value) == "mv1" &&
		len(m2.Kvs) == 1 && string(m2.Kvs[0].Value) == "mv2" {
		ok("Txn multi-op both puts applied")
	} else {
		bad("Txn multi-op both puts applied", "one or both keys missing/wrong")
	}

	// Clean up
	cli.Delete(ctx, "txn/", clientv3.WithPrefix())
}

// ============================================================
// Watch section
// ============================================================

func testWatch(cli *clientv3.Client) {
	fmt.Println("\n== WATCH ==")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ---- Live watch: PUT v1 / PUT v2 / DELETE ----
	// WithCreatedNotify() causes the client library to deliver the CREATED ack
	// to the WatchChan (by default the library consumes it internally).
	watchCtx, watchCancel := context.WithCancel(ctx)

	wch := cli.Watch(watchCtx, "watch/key1", clientv3.WithCreatedNotify())
	// Consume the CREATED ack (first message on the channel).
	created := false
	createTimeout := time.NewTimer(5 * time.Second)
waitCreate:
	for {
		select {
		case resp, open := <-wch:
			if !open {
				bad("Watch live: receive CREATED ack", "channel closed before CREATED")
				watchCancel()
				return
			}
			if resp.Created {
				created = true
				break waitCreate
			}
		case <-createTimeout.C:
			break waitCreate
		}
	}
	if created {
		ok("Watch live: received CREATED ack (WithCreatedNotify)")
	} else {
		bad("Watch live: received CREATED ack", "timeout — check WithCreatedNotify delivery")
	}

	// Fire events
	cli.Put(ctx, "watch/key1", "wv1")
	cli.Put(ctx, "watch/key1", "wv2")
	cli.Delete(ctx, "watch/key1")

	// Collect events
	var events []*clientv3.Event
	collectTimeout := time.NewTimer(8 * time.Second)
collectLoop:
	for len(events) < 3 {
		select {
		case resp, open := <-wch:
			if !open {
				break collectLoop
			}
			if resp.Err() != nil {
				bad("Watch live: collect events", resp.Err().Error())
				break collectLoop
			}
			events = append(events, resp.Events...)
		case <-collectTimeout.C:
			break collectLoop
		}
	}
	watchCancel()

	if len(events) >= 3 {
		ok(fmt.Sprintf("Watch live: collected %d events (>=3)", len(events)))
	} else {
		bad("Watch live: collected >=3 events", fmt.Sprintf("got %d", len(events)))
	}

	// Check event types and values
	putCount := 0
	delCount := 0
	for _, e := range events {
		if e.Type == clientv3.EventTypePut {
			putCount++
		} else if e.Type == clientv3.EventTypeDelete {
			delCount++
		}
	}
	if putCount >= 2 {
		ok(fmt.Sprintf("Watch live: %d PUT events", putCount))
	} else {
		bad("Watch live: >=2 PUT events", fmt.Sprintf("got %d PUTs", putCount))
	}
	if delCount >= 1 {
		ok("Watch live: >=1 DELETE event")
	} else {
		bad("Watch live: >=1 DELETE event", fmt.Sprintf("got %d DELETEs", delCount))
	}

	// Check values in PUT order
	if len(events) >= 2 {
		first, second := events[0], events[1]
		if first.Type == clientv3.EventTypePut && string(first.Kv.Value) == "wv1" &&
			second.Type == clientv3.EventTypePut && string(second.Kv.Value) == "wv2" {
			ok("Watch live: events in order wv1 < wv2")
		} else {
			v1, v2 := "", ""
			if first.Kv != nil {
				v1 = string(first.Kv.Value)
			}
			if second.Kv != nil {
				v2 = string(second.Kv.Value)
			}
			bad("Watch live: events in order wv1 < wv2",
				fmt.Sprintf("first=%q second=%q", v1, v2))
		}
	}

	// ---- Prefix watch ----
	pfxCtx, pfxCancel := context.WithCancel(ctx)
	pfxWch := cli.Watch(pfxCtx, "watch/", clientv3.WithPrefix(), clientv3.WithCreatedNotify())

	// Wait for CREATED ack (delivered because WithCreatedNotify is set).
	pfxCreateTimer := time.NewTimer(5 * time.Second)
waitPfxCreate:
	for {
		select {
		case resp, open := <-pfxWch:
			if !open {
				break waitPfxCreate
			}
			if resp.Created {
				ok("Watch prefix: received CREATED ack (WithCreatedNotify)")
				break waitPfxCreate
			}
		case <-pfxCreateTimer.C:
			bad("Watch prefix: CREATED ack", "timeout — check WithCreatedNotify delivery")
			break waitPfxCreate
		}
	}

	cli.Put(ctx, "watch/pa", "1")
	cli.Put(ctx, "watch/pb", "2")

	var pfxEvents []*clientv3.Event
	pfxTimer := time.NewTimer(8 * time.Second)
collectPfx:
	for len(pfxEvents) < 2 {
		select {
		case resp, open := <-pfxWch:
			if !open {
				break collectPfx
			}
			pfxEvents = append(pfxEvents, resp.Events...)
		case <-pfxTimer.C:
			break collectPfx
		}
	}
	pfxCancel()

	sawPA, sawPB := false, false
	for _, e := range pfxEvents {
		if e.Kv != nil {
			k := string(e.Kv.Key)
			if k == "watch/pa" {
				sawPA = true
			}
			if k == "watch/pb" {
				sawPB = true
			}
		}
	}
	if sawPA {
		ok("Watch prefix: saw watch/pa event")
	} else {
		bad("Watch prefix: saw watch/pa event", "event not received")
	}
	if sawPB {
		ok("Watch prefix: saw watch/pb event")
	} else {
		bad("Watch prefix: saw watch/pb event", "event not received")
	}

	// ---- Historical watch (WithRev) ----
	// Write three versions to a key; record the revision of the first write.
	// Then watch from that revision — the stream should replay all three.
	cli.Put(ctx, "watch/hist", "h1")
	histResp, _ := cli.Get(ctx, "watch/hist")
	var histStart int64
	if len(histResp.Kvs) == 1 {
		histStart = histResp.Kvs[0].ModRevision
	}
	cli.Put(ctx, "watch/hist", "h2")
	cli.Put(ctx, "watch/hist", "h3")

	if histStart > 0 {
		histCtx, histCancel := context.WithCancel(ctx)
		histWch := cli.Watch(histCtx, "watch/hist", clientv3.WithRev(histStart))

		var histEvents []*clientv3.Event
		histTimer := time.NewTimer(8 * time.Second)
	collectHist:
		for len(histEvents) < 3 {
			select {
			case resp, open := <-histWch:
				if !open {
					break collectHist
				}
				if resp.Err() != nil {
					bad("Watch historical: collect events", resp.Err().Error())
					break collectHist
				}
				for _, e := range resp.Events {
					if !resp.Created { // skip the CREATED ack (no events in it)
						histEvents = append(histEvents, e)
					}
				}
				// Also pick up events from the created response itself if any
				if resp.Created {
					// no events expected in created ack
				}
			case <-histTimer.C:
				break collectHist
			}
		}
		histCancel()

		var seenH1, seenH2, seenH3 bool
		for _, e := range histEvents {
			if e.Kv != nil {
				v := string(e.Kv.Value)
				if v == "h1" {
					seenH1 = true
				}
				if v == "h2" {
					seenH2 = true
				}
				if v == "h3" {
					seenH3 = true
				}
			}
		}
		if seenH1 {
			ok("Watch historical: replayed h1")
		} else {
			bad("Watch historical: replayed h1", fmt.Sprintf("histEvents=%d, vals: %v", len(histEvents), eventVals(histEvents)))
		}
		if seenH2 {
			ok("Watch historical: replayed h2")
		} else {
			bad("Watch historical: replayed h2", fmt.Sprintf("histEvents=%d, vals: %v", len(histEvents), eventVals(histEvents)))
		}
		if seenH3 {
			ok("Watch historical: replayed h3")
		} else {
			bad("Watch historical: replayed h3", fmt.Sprintf("histEvents=%d, vals: %v", len(histEvents), eventVals(histEvents)))
		}
	} else {
		bad("Watch historical: setup", "could not determine histStart revision")
	}

	// Cleanup
	cli.Delete(ctx, "watch/", clientv3.WithPrefix())
}

func eventVals(events []*clientv3.Event) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		if e.Kv != nil {
			out = append(out, string(e.Kv.Value))
		}
	}
	return out
}

// ============================================================
// Lease section
// ============================================================

func testLease(cli *clientv3.Client) {
	fmt.Println("\n== LEASE ==")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// ---- Grant ----
	grantResp, err := cli.Grant(ctx, 10)
	if !mustOK("Lease Grant(ttl=10) succeeds", err) {
		return
	}
	if grantResp.TTL == 10 {
		ok("Lease Grant TTL=10")
	} else {
		bad("Lease Grant TTL=10", fmt.Sprintf("got TTL=%d", grantResp.TTL))
	}
	leaseID := grantResp.ID
	if leaseID != 0 {
		ok(fmt.Sprintf("Lease Grant returned non-zero ID=%d", int64(leaseID)))
	} else {
		bad("Lease Grant non-zero ID", "ID=0")
	}

	// ---- Put with lease ----
	_, err = cli.Put(ctx, "lease/key", "lv", clientv3.WithLease(leaseID))
	mustOK("Put lease/key with lease succeeds", err)

	lkResp, _ := cli.Get(ctx, "lease/key")
	if len(lkResp.Kvs) == 1 && string(lkResp.Kvs[0].Value) == "lv" {
		ok("Get lease/key=lv confirmed")
	} else {
		bad("Get lease/key=lv", fmt.Sprintf("kvs=%v", lkResp.Kvs))
	}

	// ---- TimeToLive with WithAttachedKeys ----
	ttlResp, err := cli.TimeToLive(ctx, leaseID, clientv3.WithAttachedKeys())
	if mustOK("Lease TimeToLive(WithAttachedKeys) succeeds", err) {
		if ttlResp.GrantedTTL == 10 {
			ok("TimeToLive GrantedTTL=10")
		} else {
			bad("TimeToLive GrantedTTL=10", fmt.Sprintf("got %d", ttlResp.GrantedTTL))
		}
		if ttlResp.TTL > 0 {
			ok(fmt.Sprintf("TimeToLive remaining TTL=%d (>0)", ttlResp.TTL))
		} else {
			bad("TimeToLive remaining TTL>0", fmt.Sprintf("got %d", ttlResp.TTL))
		}
		foundKey := false
		for _, k := range ttlResp.Keys {
			if string(k) == "lease/key" {
				foundKey = true
			}
		}
		if foundKey {
			ok("TimeToLive WithAttachedKeys shows lease/key")
		} else {
			bad("TimeToLive WithAttachedKeys shows lease/key",
				fmt.Sprintf("keys=%v", func() []string {
					s := make([]string, len(ttlResp.Keys))
					for i, k := range ttlResp.Keys {
						s[i] = string(k)
					}
					return s
				}()))
		}
	}

	// ---- KeepAlive: consume at least 2 responses ----
	kaCtx, kaCancel := context.WithCancel(ctx)
	kaCh, err := cli.KeepAlive(kaCtx, leaseID)
	kaOK := false
	if mustOK("Lease KeepAlive starts", err) {
		received := 0
		kaTimer := time.NewTimer(12 * time.Second)
	kaLoop:
		for received < 2 {
			select {
			case resp, open := <-kaCh:
				if !open {
					bad("KeepAlive channel open for 2 responses", "channel closed early")
					break kaLoop
				}
				if resp == nil {
					// nil means lease expired; but we have 10s TTL
					bad("KeepAlive response not nil", "got nil")
					break kaLoop
				}
				received++
				if resp.TTL > 0 {
					ok(fmt.Sprintf("KeepAlive response %d: TTL=%d", received, resp.TTL))
				} else {
					bad(fmt.Sprintf("KeepAlive response %d TTL>0", received), fmt.Sprintf("TTL=%d", resp.TTL))
				}
			case <-kaTimer.C:
				if received >= 1 {
					// 1 is acceptable — the second may have a long wait (TTL/3 = ~3s)
					// but we gave 12s, so 2 should always arrive
					bad("KeepAlive received 2 responses in 12s", fmt.Sprintf("got %d", received))
				} else {
					bad("KeepAlive received at least 1 response in 12s", "none received")
				}
				break kaLoop
			}
		}
		if received >= 2 {
			kaOK = true
		}
	}
	kaCancel()
	_ = kaOK

	// ---- Leases() ----
	leasesResp, err := cli.Leases(ctx)
	if mustOK("Lease Leases() succeeds", err) {
		found := false
		for _, ls := range leasesResp.Leases {
			if ls.ID == leaseID {
				found = true
			}
		}
		if found {
			ok(fmt.Sprintf("Leases() contains our lease ID=%d", int64(leaseID)))
		} else {
			bad("Leases() contains our lease ID", fmt.Sprintf("leases=%v", leasesResp.Leases))
		}
	}

	// ---- Revoke ----
	_, err = cli.Revoke(ctx, leaseID)
	mustOK("Lease Revoke succeeds", err)

	// Key should be gone after revoke
	afterRevoke, _ := cli.Get(ctx, "lease/key")
	if len(afterRevoke.Kvs) == 0 {
		ok("Get lease/key after Revoke -> empty (lease-attached key deleted)")
	} else {
		bad("Get lease/key after Revoke -> empty", fmt.Sprintf("got %d kvs", len(afterRevoke.Kvs)))
	}

	// Revoked lease should not appear in Leases()
	leasesAfter, _ := cli.Leases(ctx)
	stillPresent := false
	for _, ls := range leasesAfter.Leases {
		if ls.ID == leaseID {
			stillPresent = true
		}
	}
	if !stillPresent {
		ok("Revoked lease no longer in Leases()")
	} else {
		bad("Revoked lease no longer in Leases()", "lease still present after revoke")
	}
}

// ============================================================
// Compact section
// ============================================================

func testCompact(cli *clientv3.Client) {
	fmt.Println("\n== COMPACT ==")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Write a key so we have a known revision to compact to
	cli.Put(ctx, "compact/k", "v1")
	cli.Put(ctx, "compact/k", "v2")

	// Get the current revision
	curResp, err := cli.Get(ctx, "compact/k")
	if !mustOK("Get compact/k for current revision", err) {
		return
	}
	curRev := curResp.Header.Revision
	fmt.Printf("  current revision = %d\n", curRev)

	// Compact to the current revision
	_, err = cli.Compact(ctx, curRev)
	mustOK(fmt.Sprintf("Compact(rev=%d) succeeds", curRev), err)

	// A read at rev=1 (below the compaction floor) must fail with ErrCompacted
	belowRev := int64(1)
	_, err = cli.Get(ctx, "compact/k", clientv3.WithRev(belowRev))
	if err != nil && isCompactedErr(err) {
		ok(fmt.Sprintf("Get WithRev(%d) after Compact(%d) -> ErrCompacted", belowRev, curRev))
	} else if err != nil {
		bad(fmt.Sprintf("Get WithRev(%d) -> ErrCompacted", belowRev),
			fmt.Sprintf("unexpected error: %v", err))
	} else {
		bad(fmt.Sprintf("Get WithRev(%d) -> ErrCompacted", belowRev), "no error returned")
	}

	// A read at the compaction floor itself should still work
	atFloor, err := cli.Get(ctx, "compact/k", clientv3.WithRev(curRev))
	if mustOK(fmt.Sprintf("Get WithRev(%d) at compaction floor succeeds", curRev), err) {
		if len(atFloor.Kvs) == 1 && string(atFloor.Kvs[0].Value) == "v2" {
			ok("Get at compaction floor returns correct value")
		} else {
			bad("Get at compaction floor returns v2", fmt.Sprintf("kvs=%v", atFloor.Kvs))
		}
	}

	// Cleanup
	cli.Delete(ctx, "compact/", clientv3.WithPrefix())
}

// ============================================================
// main
// ============================================================

func main() {
	addr := "127.0.0.1:12345"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	fmt.Printf("clientv3-compat: connecting to %s\n", addr)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 8 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: clientv3.New(%s): %v\n", addr, err)
		os.Exit(1)
	}
	defer cli.Close()

	testKV(cli)
	testTxn(cli)
	testWatch(cli)
	testLease(cli)
	testCompact(cli)

	fmt.Printf("\n================================================================\n")
	fmt.Printf("%d passed, %d failed\n", passed, failed)
	if failed > 0 {
		fmt.Println("CLIENTV3 COMPAT: FAILED")
		os.Exit(1)
	}
	fmt.Println("CLIENTV3 COMPAT: ALL PASS")
}
