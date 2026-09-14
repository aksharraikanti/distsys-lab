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
type Persister interface {
	SaveState(state []byte) error
	ReadState() ([]byte, error)
}

// MemoryPersister is an in-memory Persister. It "survives a restart"
// only in the sense that a test can construct a fresh *Raft against the
// SAME MemoryPersister instance after discarding the old one — that's
// what simulates a crash-and-restart without touching a real filesystem
// (see persist_test.go). It does NOT survive an actual process exit;
// FilePersister is what does that.
type MemoryPersister struct {
	mu    sync.Mutex
	state []byte
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

// FilePersister durably stores state on disk at a fixed path.
//
// Every SaveState writes to a temp file in the same directory, fsyncs
// it, atomically renames it over the real path, then fsyncs the
// directory too. Each step closes a distinct crash-consistency gap:
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
	path string
	mu   sync.Mutex
}

// NewFilePersister returns a FilePersister backed by the file at path.
// The containing directory must already exist.
func NewFilePersister(path string) *FilePersister {
	return &FilePersister{path: path}
}

func (p *FilePersister) SaveState(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	dir := filepath.Dir(p.path)
	tmp, err := os.CreateTemp(dir, filepath.Base(p.path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// Best-effort cleanup if anything fails before the rename. Once the
	// rename below succeeds, nothing exists under tmpName any more, so
	// this second removal attempt is a harmless no-op error we ignore.
	defer os.Remove(tmpName)

	if _, err := tmp.Write(state); err != nil {
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
	if err := os.Rename(tmpName, p.path); err != nil {
		return err
	}

	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dirFile.Close()
	return dirFile.Sync()
}

func (p *FilePersister) ReadState() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil // first-ever boot — nothing persisted yet
	}
	return data, err
}
