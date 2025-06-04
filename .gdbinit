handle SIGURG nostop noprint pass
add-auto-load-safe-path /mnt/oldrog/usr/local/go1.24.3/src/runtime/runtime-gdb.py
add-auto-load-safe-path /usr/local/go/src/runtime/runtime-gdb.py
set auto-load safe-path /
set debuginfod enabled on

#source /usr/local/go/src/runtime/runtime-gdb.py

## counter the Go-runrime fighting over signals
define hook-stop
      handle SIGURG nostop noprint pass
end
