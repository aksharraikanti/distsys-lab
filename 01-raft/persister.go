package raft

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// Persister durably stores and retrieves a Raft node's persistent state —
// currentTerm, votedFor, and log, opaquely encoded as a []byte (see
// persist.go for the encoding). SaveState is called after every mutation
// to those three fields, before that mutation becomes visible outside
// this node (an RPC reply, a Propose return). ReadState is called once,
// at construction, to recover after a restart.
//
// SaveStateAndSnapshot/ReadSnapshot (Day 6) do the same job for a
// second, independent blob: the state machine's own serialized
// snapshot, opaque to Raft itself. They're separate from
// SaveState/ReadState — rather than, say, folding the snapshot bytes
// into persistedState — because snapshots can be large and change on a
// different rhythm (whenever the size-based policy fires) than
// term/votedFor/log (every single mutation). SaveStateAndSnapshot takes
// BOTH blobs in one call, not two separate calls, because they must
// become durable together: state's LastIncludedIndex/Term is what
// claims a given prefix of the log is safely reconstructible from
// snapshot, so persisting one without the other risks a claim that
// doesn't match what's actually on disk.
type Persister interface {
	SaveState(state []byte) error
	ReadState() ([]byte, error)
	SaveStateAndSnapshot(state []byte, snapshot []byte) error
	ReadSnapshot() ([]byte, error)
}

// MemoryPersister is an in-memory Persister. It "survives a restart"
// only in the sense that a test can construct a fresh *Raft against the
// SAME MemoryPersister instance after discarding the old one — that's
// what simulates a crash-and-restart without touching a real filesystem
// (see persist_test.go). It does NOT survive an actual process exit;
// FilePersister is what does that.
type MemoryPersister struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

// NewMemoryPersister returns an empty MemoryPersister — equivalent to a
// node that has never persisted anything, i.e. a first-ever boot.
func NewMemoryPersister() *MemoryPersister {
	return &MemoryPersister{}
}

func (p *MemoryPersister) SaveState(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
	return nil
}

func (p *MemoryPersister) ReadState() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.state...), nil
}

func (p *MemoryPersister) SaveStateAndSnapshot(state, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
	p.snapshot = append([]byte(nil), snapshot...)
	return nil
}

func (p *MemoryPersister) ReadSnapshot() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.snapshot...), nil
}

// FilePersister durably stores state on disk at a fixed path, and (Day
// 6) a snapshot alongside it at that same path plus a ".snapshot"
// suffix.
//
// Every write goes through writeFileAtomicLocked: write to a temp file
// in the same directory, fsync it, atomically rename it over the real
// path, then fsync the directory too. Each step closes a distinct
// crash-consistency gap:
//
//   - Writing directly to the real path (no temp file) risks a crash
//     mid-write leaving a half-written, corrupted file with no way to
//     recover the prior good state.
//   - Renaming without fsyncing the temp file first risks the rename
//     becoming durable while the file's actual bytes are still sitting
//     in the OS page cache — a crash right after could leave the
//     renamed file's content lost or zeroed.
//   - Renaming is itself a directory-metadata change, not just a file
//     write. Without fsyncing the DIRECTORY too, a crash right after a
//     "successful" rename can — on some filesystems — still lose the
//     rename itself, even though the temp file's own contents were
//     already fsynced. This is the deeper, easy-to-miss half of what
//     "atomic rename" durability actually requires.
//
// POSIX rename is atomic on the same filesystem, so at every point
// during this sequence a reader sees either the complete OLD file or the
// complete NEW file — never a partial one.
type FilePersister struct {
	path         string
	snapshotPath string
	mu           sync.Mutex
}

// NewFilePersister returns a FilePersister backed by the file at path
// (and, for snapshots, path+".snapshot"). The containing directory must
// already exist.
func NewFilePersister(path string) *FilePersister {
	return &FilePersister{path: path, snapshotPath: path + ".snapshot"}
}

func (p *FilePersister) SaveState(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writeFileAtomicLocked(p.path, state)
}

func (p *FilePersister) ReadState() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return readFileOrEmpty(p.path)
}

// SaveStateAndSnapshot persists both blobs, snapshot FIRST and state
// SECOND — the only ordering a crash between the two can't corrupt.
// state is what "commits" the compaction: it's the one carrying
// LastIncludedIndex/Term, the claim that everything through that index
// is now reconstructible from the snapshot instead of the (now
// shorter) log. Writing state first and crashing before snapshot lands
// would leave that claim on disk with nothing to back it up —
// unrecoverable data loss on the next restore. Writing snapshot first
// means a crash in between just leaves a stray, still-unreferenced
// snapshot file behind: restoreLocked will read the OLD state (still
// describing the larger, not-yet-trimmed log), so nothing is lost
// either way.
func (p *FilePersister) SaveStateAndSnapshot(state, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.writeFileAtomicLocked(p.snapshotPath, snapshot); err != nil {
		return err
	}
	return p.writeFileAtomicLocked(p.path, state)
}

func (p *FilePersister) ReadSnapshot() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return readFileOrEmpty(p.snapshotPath)
}

// writeFileAtomicLocked is the shared temp-file/fsync/rename/fsync-dir
// sequence SaveState and SaveStateAndSnapshot both build on. Caller
// must already hold p.mu.
func (p *FilePersister) writeFileAtomicLocked(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// Best-effort cleanup if anything fails before the rename. Once the
	// rename below succeeds, nothing exists under tmpName any more, so
	// this second removal attempt is a harmless no-op error we ignore.
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}

	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dirFile.Close()
	return dirFile.Sync()
}

// readFileOrEmpty reads path, treating "doesn't exist yet" as a valid
// empty result (a first-ever boot, or a node that's never snapshotted)
// rather than an error.
func readFileOrEmpty(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return data, err
}
