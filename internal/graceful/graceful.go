package graceful

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// OnShutdown will call registered functions pass to Do when the program receives a shutdown signal. It should be
// declared once where cleanup tasks need to be registered.
type OnShutdown struct {
	sync.Mutex
	init sync.Once
	todo []func()
	// Set OsExit to true if this is declared in the main package and should call os.Exit when done.
	OsExit bool
}

func (s *OnShutdown) Do(f func()) {
	s.Lock()
	defer s.Unlock()
	s.todo = append(s.todo, f)
	s.init.Do(func() {
		go func() {
			sigc := make(chan os.Signal, 1)
			signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGSTOP)
			<-sigc
			s.Exit()
			if s.OsExit {
				os.Exit(0)
			}
		}()
	})
}

func (s *OnShutdown) Exit() {
	s.Lock()
	defer s.Unlock()
	for _, fn := range s.todo {
		fn()
	}
	s.todo = nil
}
