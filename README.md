# Rend LMDB backend

This repository shows an example Rend server with an LMDB backend (as opposed to the standard one
that speaks memcached). The code to create this version of Rend consists of an implementation of
github.com/netflix/rend/handlers.Handler only. Beyond that, there is an example server
implementation that shows how to wire up the LMDB backend with a Rend server.

The Rend project can be used as a library as well as a server. The project has a default server
which showcases the full capabilities it has, however using a library like this one, Rend can be
extended to use any backend storage.

The performance of Rend will be different based on the backend used, for example memcached stores
data only in memory but this libary could possibly store data partially on disk and partially in
memory - the same semantics as the underlying LMDB store.

## Prerequisites

Latest version of Go, which can be found at https://golang.org/

## Getting the code

`$ go get github.com/netflix/rend-lmdb`

## Building the example server

`$ go build github.com/netflix/rend-lmdb/example`

## Running the example server

```
$ umask 002 # For LMDB permissions
$ ./example
```

## Test it out

Open another console window and try it out:

```
$ nc localhost 12121
> get foo
NOT_FOUND
> set foo 0 0 6
> foobar
STORED
> get foo
END
> touch foo 2
TOUCHED
> get foo
VALUE foo 0 6
foobar
END
> get foo
END
> quit
Bye
```
