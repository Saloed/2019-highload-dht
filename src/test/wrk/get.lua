require("init")

local counter = 0
request = function()
    local path = "/v0/entity?id=" .. counter
    counter = counter + 1
    return wrk.format("GET", path)
end
