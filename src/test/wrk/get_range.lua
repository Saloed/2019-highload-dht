require("init")

local counter = 0
request = function()
    local path = string.format("/v0/entities?start=%d&end=%d", counter, counter + 1)
    counter = math.fmod(counter + 1, 99999)
    counter = math.max(counter, 10000)
    return wrk.format("GET", path)
end
