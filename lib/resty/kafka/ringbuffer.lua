-- Copyright (C) Dejiang Zhu(doujiang24)

local semaphore = require "ngx.semaphore"

local setmetatable = setmetatable
local ngx_null = ngx.null
local ngx_log = ngx.log
local WARN = ngx.WARN
local type = type
local tostring = tostring
local string_sub = string.sub

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

-- Helper function to safely truncate for logging
local function truncate_for_log(val, max_len)
    max_len = max_len or 1000
    if val == nil then
        return "nil"
    end
    if val == ngx_null then
        return "ngx.null"
    end
    local str = tostring(val)
    if #str > max_len then
        return string_sub(str, 1, max_len) .. "...[truncated, total=" .. #str .. "]"
    end
    return str
end


local _M = {}
local mt = { __index = _M }

function _M.new(self, batch_num, max_buffering, wait_on_buffer_full, wait_buffer_timeout)
    local sendbuffer = {
        queue = new_tab(max_buffering * 3, 0),
        batch_num = batch_num,
        size = max_buffering * 3,
        start = 1,
        num = 0,
        wait_on_buffer_full = wait_on_buffer_full,
        wait_buffer_timeout = wait_buffer_timeout,
    }

    if wait_on_buffer_full then
        sendbuffer.sema = semaphore.new()
    end

    return setmetatable(sendbuffer, mt)
end


function _M.add(self, topic, key, message, wait_timeout, depth)
    ngx_log(WARN, "[TRACE-RINGBUF-ADD] Called with topic_type=", type(topic), 
        ", topic=", truncate_for_log(topic, 100),
        ", key_type=", type(key), ", key=", truncate_for_log(key, 100),
        ", message_type=", type(message), ", message=", truncate_for_log(message, 200),
        ", message_len=", message and #message or 0,
        ", depth=", depth or 1)
    
    local num = self.num
    local size = self.size
    ngx_log(WARN, "[TRACE-RINGBUF-ADD] Buffer state: num=", num, ", size=", size, ", start=", self.start)

    if num >= size then
        ngx_log(WARN, "[TRACE-RINGBUF-ADD] Buffer full! num=", num, " >= size=", size)
        if not self.wait_on_buffer_full then
            ngx_log(WARN, "[TRACE-RINGBUF-ADD] Returning buffer overflow error")
            return nil, "buffer overflow"
        end

        depth = depth or 1
        if depth > 10 then
            ngx_log(WARN, "[TRACE-RINGBUF-ADD] Max depth exceeded, depth=", depth)
            return nil, "buffer overflow and over max depth"
        end

        local timeout = wait_timeout or self.wait_buffer_timeout
        if timeout <= 0 then
            ngx_log(WARN, "[TRACE-RINGBUF-ADD] Timeout expired")
             return nil, "buffer overflow and timeout"
        end

        ngx_log(WARN, "[TRACE-RINGBUF-ADD] Waiting for buffer space, timeout=", timeout)
        local start_time = ngx.now()
        local ok, err = self.sema:wait(timeout)
        if not ok then
            ngx_log(WARN, "[TRACE-RINGBUF-ADD] Semaphore wait failed: ", err)
            return nil, "buffer overflow " .. err
        end
        timeout = timeout - (ngx.now() - start_time)

        -- since sema:post to sema:wait is async, so need to check ringbuffer is available again
        ngx_log(WARN, "[TRACE-RINGBUF-ADD] Retrying add after semaphore, remaining timeout=", timeout)
        return self:add(topic, key, message, timeout, depth + 1)
    end

    local index = (self.start + num) % size
    local queue = self.queue

    ngx_log(WARN, "[TRACE-RINGBUF-ADD] Storing at indices: ", index, " (topic), ", index + 1, " (key), ", index + 2, " (message)")
    ngx_log(WARN, "[TRACE-RINGBUF-ADD] Before store: queue[", index, "]=", truncate_for_log(queue[index], 100),
        ", queue[", index + 1, "]=", truncate_for_log(queue[index + 1], 100),
        ", queue[", index + 2, "]=", truncate_for_log(queue[index + 2], 100))
    
    queue[index] = topic
    queue[index + 1] = key
    queue[index + 2] = message

    ngx_log(WARN, "[TRACE-RINGBUF-ADD] After store: queue[", index, "]=", truncate_for_log(queue[index], 100),
        ", queue[", index + 1, "]=", truncate_for_log(queue[index + 1], 100),
        ", queue[", index + 2, "]=", truncate_for_log(queue[index + 2], 100))

    self.num = num + 3
    ngx_log(WARN, "[TRACE-RINGBUF-ADD] Updated num to ", self.num, ", left_num=", self.num / 3)

    return true
end


function _M.release_buffer_wait(self)
    if not self.wait_on_buffer_full then
        return
    end

    -- It is enough to release a waiter as only one message pops up
    if self.sema:count() < 0 then
        self.sema:post(1)
    end
end


function _M.pop(self)
    local num = self.num
    ngx_log(WARN, "[TRACE-RINGBUF-POP] Called, num=", num, ", start=", self.start)
    if num <= 0 then
        ngx_log(WARN, "[TRACE-RINGBUF-POP] Buffer empty, returning nil")
        return nil, "empty buffer"
    end

    self.num = num - 3

    local start = self.start
    local queue = self.queue

    self.start = (start + 3) % self.size

    ngx_log(WARN, "[TRACE-RINGBUF-POP] Reading from indices: ", start, ", ", start + 1, ", ", start + 2)
    ngx_log(WARN, "[TRACE-RINGBUF-POP] Raw values: queue[", start, "]=", truncate_for_log(queue[start], 100),
        ", queue[", start + 1, "]=", truncate_for_log(queue[start + 1], 100),
        ", queue[", start + 2, "]=", truncate_for_log(queue[start + 2], 100))
    
    -- NOTE: Variable names here are misleading but return order is correct
    -- queue[start] = topic (stored in 'key' variable)
    -- queue[start + 1] = key (stored in 'topic' variable)
    -- queue[start + 2] = message
    local key, topic, message = queue[start], queue[start + 1], queue[start + 2]
    
    ngx_log(WARN, "[TRACE-RINGBUF-POP] Assigned to variables: key(actually topic)=", truncate_for_log(key, 100),
        ", topic(actually key)=", truncate_for_log(topic, 100),
        ", message=", truncate_for_log(message, 200))
    ngx_log(WARN, "[TRACE-RINGBUF-POP] Types: key_type=", type(key), 
        ", topic_type=", type(topic), ", message_type=", type(message))

    queue[start], queue[start + 1], queue[start + 2] = ngx_null, ngx_null, ngx_null
    ngx_log(WARN, "[TRACE-RINGBUF-POP] Cleared queue slots, new start=", self.start, ", new num=", self.num)

    self:release_buffer_wait()

    ngx_log(WARN, "[TRACE-RINGBUF-POP] Returning: (", truncate_for_log(key, 100), ", ", 
        truncate_for_log(topic, 100), ", ", truncate_for_log(message, 100), ")")
    return key, topic, message
end


function _M.left_num(self)
    return self.num / 3
end


function _M.need_send(self)
    return self.num / 3 >= self.batch_num
end


return _M
