-- Copyright (C) Dejiang Zhu(doujiang24)


local setmetatable = setmetatable
local pairs = pairs
local next = next
local ngx_log = ngx.log
local WARN = ngx.WARN
local type = type
local tostring = tostring
local string_sub = string.sub


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local MAX_REUSE = 10000

-- Helper function to safely truncate for logging
local function truncate_for_log(val, max_len)
    max_len = max_len or 1000
    if val == nil then
        return "nil"
    end
    local str = tostring(val)
    if #str > max_len then
        return string_sub(str, 1, max_len) .. "...[truncated, total=" .. #str .. "]"
    end
    return str
end


local _M = {}
local mt = { __index = _M }

function _M.new(self, batch_num, batch_size)
    local sendbuffer = {
        topics = {},
        queue_num = 0,
        batch_num = batch_num * 2,
        batch_size = batch_size,
    }
    return setmetatable(sendbuffer, mt)
end


function _M.add(self, topic, partition_id, key, msg)
    ngx_log(WARN, "[TRACE-SENDBUF-ADD] Called with topic=", truncate_for_log(topic, 100), 
        ", partition_id=", partition_id,
        ", key_type=", type(key), ", key=", truncate_for_log(key, 100),
        ", msg_type=", type(msg), ", msg=", truncate_for_log(msg, 200),
        ", msg_len=", msg and #msg or 0)
    
    local topics = self.topics

    if not topics[topic] then
        ngx_log(WARN, "[TRACE-SENDBUF-ADD] Creating new topic entry for: ", truncate_for_log(topic, 100))
        topics[topic] = {}
    end

    if not topics[topic][partition_id] then
        ngx_log(WARN, "[TRACE-SENDBUF-ADD] Creating new partition entry for topic=", 
            truncate_for_log(topic, 100), ", partition_id=", partition_id)
        topics[topic][partition_id] = {
            queue = new_tab(self.batch_num, 0),
            index = 0,
            used = 0,
            size = 0,
            offset = 0,
            retryable = true,
            err = "",
        }
    end

    local buffer = topics[topic][partition_id]
    local index = buffer.index
    local queue = buffer.queue

    if index == 0 then
        self.queue_num = self.queue_num + 1
        buffer.retryable = true
        ngx_log(WARN, "[TRACE-SENDBUF-ADD] First message in buffer, queue_num now=", self.queue_num)
    end

    ngx_log(WARN, "[TRACE-SENDBUF-ADD] Storing at index ", index + 1, " (key) and ", index + 2, " (msg)")
    ngx_log(WARN, "[TRACE-SENDBUF-ADD] Before store: queue[", index + 1, "]=", truncate_for_log(queue[index + 1], 100),
        ", queue[", index + 2, "]=", truncate_for_log(queue[index + 2], 100))
    
    queue[index + 1] = key
    queue[index + 2] = msg

    ngx_log(WARN, "[TRACE-SENDBUF-ADD] After store: queue[", index + 1, "]=", truncate_for_log(queue[index + 1], 100),
        ", queue[", index + 2, "]=", truncate_for_log(queue[index + 2], 100))

    buffer.index = index + 2
    buffer.size = buffer.size + #msg + (key and #key or 0)

    ngx_log(WARN, "[TRACE-SENDBUF-ADD] Buffer updated: index=", buffer.index, 
        ", size=", buffer.size, ", msg_count=", buffer.index / 2,
        ", batch_size=", self.batch_size, ", batch_num=", self.batch_num)

    if (buffer.size >= self.batch_size) or (buffer.index >= self.batch_num) then
        ngx_log(WARN, "[TRACE-SENDBUF-ADD] Buffer overflow condition met, returning true")
        return true
    end
    ngx_log(WARN, "[TRACE-SENDBUF-ADD] Completed, no overflow")
end


function _M.offset(self, topic, partition_id, offset)
    local buffer = self.topics[topic][partition_id]

    if not offset then
        return buffer.offset
    end

    buffer.offset = offset + (buffer.index / 2)
end


function _M.clear(self, topic, partition_id)
    local buffer = self.topics[topic][partition_id]
    buffer.index = 0
    buffer.size = 0
    buffer.used = buffer.used + 1

    if buffer.used >= MAX_REUSE then
        buffer.queue = new_tab(self.batch_num, 0)
        buffer.used = 0
    end

    self.queue_num = self.queue_num - 1
end


function _M.done(self)
    return self.queue_num == 0
end


function _M.err(self, topic, partition_id, err, retryable)
    local buffer = self.topics[topic][partition_id]

    if err then
        ngx_log(WARN, "[TRACE-SENDBUF-ERR] Setting error for topic=", truncate_for_log(topic, 100), 
            ", partition_id=", partition_id, ", err=", err, ", retryable=", tostring(retryable),
            ", buffer.index=", buffer.index, ", msg_count=", buffer.index / 2)
        buffer.err = err
        buffer.retryable = retryable
        return buffer.index
    else
        ngx_log(WARN, "[TRACE-SENDBUF-ERR] Getting error for topic=", truncate_for_log(topic, 100), 
            ", partition_id=", partition_id, ": err=", buffer.err or "nil", 
            ", retryable=", tostring(buffer.retryable))
        return buffer.err, buffer.retryable
    end
end


function _M.loop(self)
    local topics, t, p = self.topics

    return function ()
        if t then
            for partition_id, queue in next, topics[t], p do
                p = partition_id
                if queue.index > 0 then
                    return t, partition_id, queue
                end
            end
        end


        for topic, partitions in next, topics, t do
            t = topic
            p = nil
            for partition_id, queue in next, partitions, p do
                p = partition_id
                if queue.index > 0 then
                    return topic, partition_id, queue
                end
            end
        end

        return
    end
end


function _M.aggregator(self, client)
    ngx_log(WARN, "[TRACE-SENDBUF-AGG] Starting aggregator, queue_num=", self.queue_num)
    local num = 0
    local sendbroker = {}
    local brokers = {}

    local i = 1
    for topic, partition_id, queue in self:loop() do
        ngx_log(WARN, "[TRACE-SENDBUF-AGG] Processing topic=", truncate_for_log(topic, 100), 
            ", partition_id=", partition_id, ", queue.index=", queue.index, 
            ", queue.retryable=", tostring(queue.retryable), ", queue.err=", queue.err or "nil")
        
        -- Log messages in this queue
        for j = 1, queue.index, 2 do
            local qkey = queue.queue[j]
            local qmsg = queue.queue[j + 1]
            ngx_log(WARN, "[TRACE-SENDBUF-AGG] Queue message[", (j + 1) / 2, "] key_type=", type(qkey),
                ", key=", truncate_for_log(qkey, 100),
                ", msg_type=", type(qmsg),
                ", msg=", truncate_for_log(qmsg, 200))
        end
        
        if queue.retryable then
            local broker_conf, err = client:choose_broker(topic, partition_id)
            ngx_log(WARN, "[TRACE-SENDBUF-AGG] choose_broker returned: broker=", 
                broker_conf and (broker_conf.host .. ":" .. broker_conf.port) or "nil",
                ", err=", err or "nil")
            if not broker_conf then
                ngx_log(WARN, "[TRACE-SENDBUF-AGG] No broker found, setting error")
                self:err(topic, partition_id, err, true)

            else
                if not brokers[broker_conf] then
                    ngx_log(WARN, "[TRACE-SENDBUF-AGG] Creating new broker entry for ", 
                        broker_conf.host, ":", broker_conf.port)
                    brokers[broker_conf] = {
                        topics = {},
                        topic_num = 0,
                        size = 0,
                    }
                end

                local broker = brokers[broker_conf]
                if not broker.topics[topic] then
                    brokers[broker_conf].topics[topic] = {
                        partitions = {},
                        partition_num = 0,
                    }

                    broker.topic_num = broker.topic_num + 1
                end

                local broker_topic = broker.topics[topic]

                broker_topic.partitions[partition_id] = queue
                broker_topic.partition_num = broker_topic.partition_num + 1

                broker.size = broker.size + queue.size
                ngx_log(WARN, "[TRACE-SENDBUF-AGG] Added to broker, broker.size=", broker.size,
                    ", batch_size=", self.batch_size)

                if broker.size >= self.batch_size then
                    ngx_log(WARN, "[TRACE-SENDBUF-AGG] Broker batch full, adding to sendbroker")
                    sendbroker[num + 1] = broker_conf
                    sendbroker[num + 2] = brokers[broker_conf]

                    num = num + 2
                    brokers[broker_conf] = nil
                end
            end
        else
            ngx_log(WARN, "[TRACE-SENDBUF-AGG] Skipping non-retryable queue")
        end
    end

    for broker_conf, topic_partitions in pairs(brokers) do
        ngx_log(WARN, "[TRACE-SENDBUF-AGG] Adding remaining broker ", 
            broker_conf.host, ":", broker_conf.port, " to sendbroker")
        sendbroker[num + 1] = broker_conf
        sendbroker[num + 2] = brokers[broker_conf]
        num = num + 2
    end

    ngx_log(WARN, "[TRACE-SENDBUF-AGG] Completed, returning num=", num)
    return num, sendbroker
end


return _M
