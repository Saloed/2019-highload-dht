local ports = { "8080", "8081", "8082" }
local thread_count = 0

function setup(thread)
    local port_idx = math.fmod(thread_count, 3) + 1
    local port = ports[port_idx]
    thread.addr = wrk.lookup(wrk.host, port)[1]
    thread_count = thread_count + 1
end

function init(args)
    local msg = "thread addr: %s"
    print(msg:format(wrk.thread.addr))
end
