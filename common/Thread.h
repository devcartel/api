#ifndef __Thread_h_
#define __Thread_h_

// A class that encapsulates the thread creation/destruction/prioritization
// logic across Win32 and Unix

#include <assert.h>

#ifdef WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <process.h>
#include <windows.h>
#else
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <signal.h>
#include <strings.h>
#include <sys/time.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#endif

class CThread
{
public:

	CThread();
	virtual ~CThread();

	virtual void start();
	virtual void stop();
	virtual void wait();

	virtual bool setPriority( int );
	void Sleep( unsigned long );

protected:
	bool isStopping();
	virtual void run() = 0;

private:
	bool	_isStopping;
	bool	_isActive;
	int		_priority;

#ifdef WIN32
	static unsigned __stdcall runThread( void* arg );
	HANDLE					_handle;
	unsigned				_threadId;
#else
public:
	static void* runThread( void* arg );
private:
	struct sched_param		m_param;
	pthread_attr_t			m_attr;
	pthread_t				_threadId;
#endif

private:
	// Declared, but not implemented to prevent default behavior generated by compiler
	CThread( const CThread & );
	CThread & operator=( const CThread & );
};

#endif // __Thread_h_
