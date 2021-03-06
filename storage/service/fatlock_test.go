package service_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/service"
)

type testLocker struct {
	ses         *session
	lockerState service.LockerState
}

func (tl *testLocker) LockerState() *service.LockerState {
	return &tl.lockerState
}

func (tl *testLocker) String() string {
	return fmt.Sprintf("locker-%d", tl.ses.id)
}

type session struct {
	id int
	tl *testLocker
}

func (_ *session) Context() context.Context {
	return nil
}

func (ses *session) String() string {
	return fmt.Sprintf("session-%d", ses.id)
}

var sessions [10]session

func getSession(ses int) *session {
	sessions[ses].id = ses
	return &sessions[ses]
}

type testStep interface {
	step(t *testing.T, svc *service.LockService)
}

type stepLockTable struct {
	ses  int
	tbl  sql.Identifier
	ll   service.LockLevel
	fail bool
	wg   *sync.WaitGroup
}

func (slt stepLockTable) lockTable(t *testing.T, ses *session, svc *service.LockService) {
	t.Helper()

	tn := sql.TableName{sql.ID("db"), sql.PUBLIC, slt.tbl}
	err := svc.LockTable(context.Background(), ses.tl, tn, slt.ll)
	if slt.fail {
		if err == nil {
			t.Errorf("LockTable(%s, %s, %s) did not fail", ses, ses.tl, slt.ll)
		}
	} else if err != nil {
		t.Errorf("LockTable(%s, %s, %s) failed with %s", ses, ses.tl, slt.ll, err)
	}
}

func (slt stepLockTable) step(t *testing.T, svc *service.LockService) {
	t.Helper()

	ses := getSession(slt.ses)
	if ses.tl == nil {
		ses.tl = &testLocker{ses: ses}
	}

	if slt.wg != nil {
		slt.wg.Add(1)

		go func() {
			defer slt.wg.Done()

			slt.lockTable(t, ses, svc)
		}()
	} else {
		slt.lockTable(t, ses, svc)
	}
}

type stepReleaseLocks struct {
	ses  int
	fail bool
	keep bool
}

func (srl stepReleaseLocks) step(t *testing.T, svc *service.LockService) {
	t.Helper()

	ses := getSession(srl.ses)
	err := svc.ReleaseLocks(ses.tl)
	if srl.fail {
		if err == nil {
			t.Errorf("ReleaseLocks(%s) did not fail", ses.tl)
		}
	} else if err != nil {
		t.Errorf("ReleaseLocks(%s) failed with %s", ses.tl, err)
	}
	if !srl.keep {
		ses.tl = nil
	}
}

type stepWait struct {
	wg *sync.WaitGroup
}

func (sw stepWait) step(t *testing.T, svc *service.LockService) {
	t.Helper()

	sw.wg.Wait()
}

type stepLocks []service.Lock

func (sl stepLocks) Len() int {
	return len(sl)
}

func (sl stepLocks) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}

func (sl stepLocks) Less(i, j int) bool {
	if sl[i].Key < sl[j].Key {
		return true
	} else if sl[i].Key > sl[j].Key {
		return false
	}

	if sl[i].Locker < sl[j].Locker {
		return true
	} else if sl[i].Locker > sl[j].Locker {
		return false
	}

	if sl[i].Level < sl[j].Level {
		return true
	} else if sl[i].Level > sl[j].Level {
		return false
	}

	return sl[i].Place < sl[j].Place
}

func (sl stepLocks) step(t *testing.T, svc *service.LockService) {
	t.Helper()

	lks := svc.Locks()

	sort.Sort(sl)
	sort.Sort((stepLocks)(lks))

	wnt := ([]service.Lock)(sl)
	if !reflect.DeepEqual(lks, wnt) {
		t.Errorf("Locks() got %#v want %#v", lks, wnt)
	}
}

type stepSleep struct{}

func (_ stepSleep) step(t *testing.T, svc *service.LockService) {
	t.Helper()

	time.Sleep(20 * time.Millisecond)
}

func TestService1(t *testing.T) {
	tbl1 := sql.ID("tbl1")
	tbl2 := sql.ID("tbl2")

	steps := []testStep{
		stepLockTable{ses: 0, tbl: tbl1, ll: service.ACCESS},
		stepLocks{{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ACCESS}},
		stepReleaseLocks{ses: 0},

		stepLockTable{ses: 0, tbl: tbl1, ll: service.EXCLUSIVE},
		stepLocks{{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.EXCLUSIVE}},
		stepLockTable{ses: 1, tbl: tbl2, ll: service.EXCLUSIVE},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.EXCLUSIVE},
			{Key: "table db.public.tbl2", Locker: "locker-1", Level: service.EXCLUSIVE},
		},
		stepReleaseLocks{ses: 0},
		stepReleaseLocks{ses: 1},

		stepLockTable{ses: 0, tbl: tbl1, ll: service.ACCESS},
		stepLocks{{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ACCESS}},
		stepReleaseLocks{ses: 0, keep: true},
		stepLockTable{ses: 0, tbl: tbl1, ll: service.ACCESS, fail: true},
		stepLocks(nil),
		stepReleaseLocks{ses: 0, fail: true},

		stepLockTable{ses: 0, tbl: tbl1, ll: service.ACCESS},
		stepLockTable{ses: 0, tbl: tbl1, ll: service.ACCESS},
		stepLockTable{ses: 1, tbl: tbl1, ll: service.ACCESS},
		stepLockTable{ses: 2, tbl: tbl1, ll: service.ACCESS},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ACCESS},
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.ACCESS},
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.ACCESS},
		},
		stepReleaseLocks{ses: 1},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ACCESS},
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.ACCESS},
		},
		stepReleaseLocks{ses: 2},
		stepLocks{{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ACCESS}},
		stepReleaseLocks{ses: 0},
		stepLocks(nil),

		stepLockTable{ses: 0, tbl: tbl1, ll: service.ACCESS},
		stepLocks{{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ACCESS}},
		stepLockTable{ses: 1, tbl: tbl1, ll: service.ROW_MODIFY},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ACCESS},
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.ROW_MODIFY},
		},
		stepLockTable{ses: 0, tbl: tbl1, ll: service.ROW_MODIFY},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ROW_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.ROW_MODIFY},
		},
		stepLockTable{ses: 2, tbl: tbl1, ll: service.ACCESS},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ROW_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.ROW_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.ACCESS},
		},
		stepReleaseLocks{ses: 1},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ROW_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.ACCESS},
		},
		stepReleaseLocks{ses: 2},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ROW_MODIFY},
		},
		stepReleaseLocks{ses: 0},

		stepLockTable{ses: 0, tbl: tbl1, ll: service.ACCESS},
		stepLockTable{ses: 1, tbl: tbl1, ll: service.METADATA_MODIFY},
		stepLockTable{ses: 0, tbl: tbl1, ll: service.ROW_MODIFY, fail: true},
		stepLockTable{ses: 2, tbl: tbl1, ll: service.ACCESS},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ACCESS},
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.METADATA_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.ACCESS},
		},
		stepReleaseLocks{ses: 2},
		stepReleaseLocks{ses: 1},
		stepReleaseLocks{ses: 0},

		stepLockTable{ses: 0, tbl: tbl1, ll: service.ACCESS},
		stepLockTable{ses: 0, tbl: tbl1, ll: service.METADATA_MODIFY},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.METADATA_MODIFY},
		},
		stepReleaseLocks{ses: 0},
	}

	var svc service.LockService
	svc.Init()
	for _, ts := range steps {
		ts.step(t, &svc)
	}
}

func TestService2(t *testing.T) {
	tbl1 := sql.ID("tbl1")

	var wg sync.WaitGroup
	steps := []testStep{
		stepLockTable{ses: 0, tbl: tbl1, ll: service.METADATA_MODIFY},
		stepLockTable{ses: 1, tbl: tbl1, ll: service.ROW_MODIFY, wg: &wg},
		stepSleep{},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.METADATA_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.ROW_MODIFY, Place: 1},
		},
		stepReleaseLocks{ses: 0},
		stepWait{wg: &wg},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.ROW_MODIFY},
		},
		stepReleaseLocks{ses: 1},
	}

	var svc service.LockService
	svc.Init()
	for _, ts := range steps {
		ts.step(t, &svc)
	}
}

func TestService3(t *testing.T) {
	tbl1 := sql.ID("tbl1")

	var wg sync.WaitGroup
	steps := []testStep{
		stepLockTable{ses: 0, tbl: tbl1, ll: service.METADATA_MODIFY},
		stepLockTable{ses: 1, tbl: tbl1, ll: service.ROW_MODIFY, wg: &wg},
		stepSleep{},
		stepLockTable{ses: 2, tbl: tbl1, ll: service.ACCESS, wg: &wg},
		stepSleep{},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.METADATA_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.ROW_MODIFY, Place: 1},
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.ACCESS, Place: 2},
		},
		stepReleaseLocks{ses: 0},
		stepWait{wg: &wg},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.ROW_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.ACCESS},
		},
		stepReleaseLocks{ses: 1},
		stepReleaseLocks{ses: 2},
	}

	var svc service.LockService
	svc.Init()
	for _, ts := range steps {
		ts.step(t, &svc)
	}
}

func TestService4(t *testing.T) {
	tbl1 := sql.ID("tbl1")

	var wg, wg2 sync.WaitGroup
	steps := []testStep{
		stepLockTable{ses: 0, tbl: tbl1, ll: service.METADATA_MODIFY},
		stepLockTable{ses: 1, tbl: tbl1, ll: service.ROW_MODIFY, wg: &wg},
		stepSleep{},
		stepLockTable{ses: 2, tbl: tbl1, ll: service.METADATA_MODIFY, wg: &wg2},
		stepSleep{},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.METADATA_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.ROW_MODIFY, Place: 1},
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.METADATA_MODIFY,
				Place: 2},
		},
		stepReleaseLocks{ses: 0},
		stepWait{wg: &wg},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.ROW_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.METADATA_MODIFY,
				Place: 1},
		},
		stepReleaseLocks{ses: 1},
		stepWait{wg: &wg2},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.METADATA_MODIFY},
		},
		stepReleaseLocks{ses: 2},
	}

	var svc service.LockService
	svc.Init()
	for _, ts := range steps {
		ts.step(t, &svc)
	}
}

func TestService5(t *testing.T) {
	tbl1 := sql.ID("tbl1")

	var wg sync.WaitGroup
	steps := []testStep{
		stepLockTable{ses: 0, tbl: tbl1, ll: service.ROW_MODIFY},
		stepLockTable{ses: 1, tbl: tbl1, ll: service.METADATA_MODIFY, wg: &wg},
		stepSleep{},
		stepLockTable{ses: 0, tbl: tbl1, ll: service.METADATA_MODIFY, fail: true},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ROW_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.METADATA_MODIFY,
				Place: 1},
		},
		stepReleaseLocks{ses: 0},
		stepWait{wg: &wg},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.METADATA_MODIFY},
		},
		stepReleaseLocks{ses: 1},
	}

	var svc service.LockService
	svc.Init()
	for _, ts := range steps {
		ts.step(t, &svc)
	}
}

func TestService6(t *testing.T) {
	tbl1 := sql.ID("tbl1")

	var wg sync.WaitGroup
	steps := []testStep{
		stepLockTable{ses: 0, tbl: tbl1, ll: service.ROW_MODIFY},
		stepLockTable{ses: 1, tbl: tbl1, ll: service.ROW_MODIFY},
		stepLockTable{ses: 2, tbl: tbl1, ll: service.METADATA_MODIFY, wg: &wg},
		stepSleep{},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-0", Level: service.ROW_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.ROW_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.METADATA_MODIFY,
				Place: 1},
		},
		stepReleaseLocks{ses: 0},
		stepSleep{},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-1", Level: service.ROW_MODIFY},
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.METADATA_MODIFY,
				Place: 1},
		},
		stepReleaseLocks{ses: 1},
		stepWait{wg: &wg},
		stepLocks{
			{Key: "table db.public.tbl1", Locker: "locker-2", Level: service.METADATA_MODIFY},
		},
		stepReleaseLocks{ses: 2},
	}

	var svc service.LockService
	svc.Init()
	for _, ts := range steps {
		ts.step(t, &svc)
	}
}
