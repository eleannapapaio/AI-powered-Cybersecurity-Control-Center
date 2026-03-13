-- wrap_logs: serialise a Fluent Bit record to a JSON string
-- and store it in the "log" field so the HTTP output sends
-- a simple { "log": "<json string>" } payload to the producer.

local function serialize(val)
    local t = type(val)
    if t == "string" then
        val = val:gsub('\\', '\\\\')
                 :gsub('"',  '\\"')
                 :gsub('\n', '\\n')
                 :gsub('\r', '\\r')
                 :gsub('\t', '\\t')
        return '"' .. val .. '"'
    elseif t == "number" then
        return tostring(val)
    elseif t == "boolean" then
        return tostring(val)
    elseif t == "table" then
        local parts = {}
        for k, v in pairs(val) do
            table.insert(parts, '"' .. tostring(k) .. '":' .. serialize(v))
        end
        return '{' .. table.concat(parts, ',') .. '}'
    else
        return '"' .. tostring(val) .. '"'
    end
end

function wrap_logs(tag, timestamp, record)
    local new_record = {}
    new_record["log"]  = serialize(record)   -- serialised original record
    new_record["tag"]  = tag
    -- return 1 = record modified, keep original timestamp
    return 1, timestamp, new_record
end