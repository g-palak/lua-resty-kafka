-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"
local request = require "resty.kafka.request"
local broker = require "resty.kafka.broker"
local client = require "resty.kafka.client"
local Errors = require "resty.kafka.errors"
local sendbuffer = require "resty.kafka.sendbuffer"
local ringbuffer = require "resty.kafka.ringbuffer"


local setmetatable = setmetatable
local timer_at = ngx.timer.at
local timer_every = ngx.timer.every
local is_exiting = ngx.worker.exiting
local ngx_sleep = ngx.sleep
local ngx_log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO
local DEBUG = ngx.DEBUG
local WARN = ngx.WARN
local debug = ngx.config.debug
local crc32 = ngx.crc32_short
local pcall = pcall
local pairs = pairs
local type = type
local tostring = tostring
local string_sub = string.sub

-- Helper function to safely truncate message for logging
local function truncate_for_log(val, max_len)
    max_len = 2000
    if val == nil then
        return "nil"
    end
    local str = tostring(val)
    if #str > max_len then
        return string_sub(str, 1, max_len) .. "...[truncated, total=" .. #str .. "]"
    end
    return str
end

local API_VERSION_V0 = 0
local API_VERSION_V1 = 1
local API_VERSION_V2 = 2

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = { _VERSION = "0.20" }
local mt = { __index = _M }


-- weak value table is useless here, cause _timer_flush always ref p
-- so, weak value table won't works
local cluster_inited = {}
local DEFAULT_CLUSTER_NAME = 1


local function default_partitioner(key, num, correlation_id)
    local id = key and crc32(key) or correlation_id

    -- partition_id is continuous and start from 0
    return id % num
end


local function correlation_id(self)
    local id = (self.correlation_id + 1) % 1073741824 -- 2^30
    self.correlation_id = id

    return id
end


local function produce_encode(self, topic_partitions)
    ngx_log(WARN, "[TRACE-ENCODE] Starting produce_encode, topic_num=", topic_partitions.topic_num)
    
    local req = request:new(request.ProduceRequest,
                            correlation_id(self), self.client.client_id, self.api_version)

    req:int16(self.required_acks)
    req:int32(self.request_timeout)
    req:int32(topic_partitions.topic_num)

    for topic, partitions in pairs(topic_partitions.topics) do
        ngx_log(WARN, "[TRACE-ENCODE] Encoding topic=", topic, ", partition_num=", partitions.partition_num)
        req:string(topic)
        req:int32(partitions.partition_num)

        for partition_id, buffer in pairs(partitions.partitions) do
            ngx_log(WARN, "[TRACE-ENCODE] Encoding partition_id=", partition_id, 
                ", buffer.index=", buffer.index, ", msg_count=", buffer.index / 2)
            
            -- Log each message in the buffer for debugging
            local queue = buffer.queue
            for i = 1, buffer.index, 2 do
                local key = queue[i]
                local msg = queue[i + 1]
                ngx_log(WARN, "[TRACE-ENCODE] Message[", (i + 1) / 2, "] key_type=", type(key),
                    ", key=", truncate_for_log(key, 1000),
                    ", msg_type=", type(msg),
                    ", msg=", truncate_for_log(msg, 2000))
            end
            
            req:int32(partition_id)

            -- MessageSetSize and MessageSet
            req:message_set(buffer.queue, buffer.index)
        end
    end

    ngx_log(WARN, "[TRACE-ENCODE] produce_encode completed")
    return req
end


local function produce_decode(resp)
    ngx_log(WARN, "[TRACE-DECODE] Starting produce_decode")
    local topic_num = resp:int32()
    ngx_log(WARN, "[TRACE-DECODE] Response contains ", topic_num, " topics")
    local ret = new_tab(0, topic_num)
    local api_version = resp.api_version

    for i = 1, topic_num do
        local topic = resp:string()
        local partition_num = resp:int32()
        ngx_log(WARN, "[TRACE-DECODE] Topic: ", topic, ", partitions: ", partition_num)

        ret[topic] = {}

        -- ignore ThrottleTime
        for j = 1, partition_num do
            local partition = resp:int32()
            local errcode, offset, timestamp

            if api_version == API_VERSION_V0 or api_version == API_VERSION_V1 then
                ret[topic][partition] = {
                    errcode = resp:int16(),
                    offset = resp:int64(),
                }

            elseif api_version == API_VERSION_V2 then
                ret[topic][partition] = {
                    errcode = resp:int16(),
                    offset = resp:int64(),
                    timestamp = resp:int64(), -- If CreateTime is used, this field is always -1
                }
            end
        end
    end

    ngx_log(WARN, "[TRACE-DECODE] produce_decode completed")
    return ret
end


local function choose_partition(self, topic, key)
    local brokers, partitions = self.client:fetch_metadata(topic)
    if not brokers then
        return nil, partitions
    end

    return self.partitioner(key, partitions.num, self.correlation_id)
end


local function _flush_lock(self)
    if not self.flushing then
        if debug then
            ngx_log(DEBUG, "flush lock accquired")
        end
        self.flushing = true
        return true
    end
    return false
end


local function _flush_unlock(self)
    if debug then
        ngx_log(DEBUG, "flush lock released")
    end
    self.flushing = false
end


local function _send(self, broker_conf, topic_partitions)
    ngx_log(WARN, "[TRACE-SEND] Starting _send to broker host=", broker_conf.host, 
        ", port=", broker_conf.port, ", topic_num=", topic_partitions.topic_num)
    
    local sendbuffer = self.sendbuffer
    local resp, retryable = nil, true
    local bk, err = broker:new(broker_conf.host, broker_conf.port, self.socket_config, broker_conf.sasl_config)
    if bk then
        ngx_log(WARN, "[TRACE-SEND] Broker connection established, calling produce_encode")
        local req = produce_encode(self, topic_partitions)
        ngx_log(WARN, "[TRACE-SEND] produce_encode returned, calling send_receive")

        resp, err, retryable = bk:send_receive(req)
        ngx_log(WARN, "[TRACE-SEND] send_receive returned, resp=", resp and "yes" or "nil", 
            ", err=", err or "nil", ", retryable=", tostring(retryable))
        
        if resp then
            local result = produce_decode(resp)
            ngx_log(WARN, "[TRACE-SEND] produce_decode completed")

            for topic, partitions in pairs(result) do
                for partition_id, r in pairs(partitions) do
                    local errcode = r.errcode
                    ngx_log(WARN, "[TRACE-SEND] Response for topic=", topic, 
                        ", partition_id=", partition_id, ", errcode=", errcode, ", offset=", tostring(r.offset))

                    if errcode == 0 then
                        sendbuffer:offset(topic, partition_id, r.offset)
                        sendbuffer:clear(topic, partition_id)
                        ngx_log(WARN, "[TRACE-SEND] Successfully sent, buffer cleared for topic=", topic, ", partition_id=", partition_id)
                    else
                        err = Errors[errcode] or Errors[-1]

                        -- set retries according to the error list
                        local retryable0 = retryable or err.retriable

                        local index = sendbuffer:err(topic, partition_id, err.msg, retryable0)

                        ngx_log(WARN, "[TRACE-SEND] Error from Kafka: ", err.msg, "(", errcode, "), retryable: ",
                            retryable0, ", topic: ", topic, ", partition_id: ", partition_id, ", length: ", index / 2)
                        ngx_log(INFO, "retry to send messages to kafka err: ", err.msg, "(", errcode, "), retryable: ",
                            retryable0, ", topic: ", topic, ", partition_id: ", partition_id, ", length: ", index / 2)
                        
                        -- Force immediate metadata refresh for NOT_LEADER_OR_FOLLOWER errors
                        -- if errcode == 6 then
                        --     ngx_log(WARN, "[TRACE-SEND] NOT_LEADER_OR_FOLLOWER detected, forcing immediate metadata refresh")
                        --     ngx_sleep(0.1)  -- Small delay to allow metadata to propagate
                        --     self.client:refresh()
                        --     ngx_log(WARN, "[TRACE-SEND] Metadata refresh completed after NOT_LEADER_OR_FOLLOWER")
                        -- end
                    end
                end
            end

            return
        end
    else
        ngx_log(WARN, "[TRACE-SEND] Failed to create broker connection, err=", err or "nil")
    end

    -- when broker new failed or send_receive failed
    ngx_log(WARN, "[TRACE-SEND] Marking all partitions with error, err=", err or "nil", ", retryable=", tostring(retryable))
    for topic, partitions in pairs(topic_partitions.topics) do
        for partition_id, partition in pairs(partitions.partitions) do
            ngx_log(WARN, "[TRACE-SEND] Setting error for topic=", topic, ", partition_id=", partition_id)
            sendbuffer:err(topic, partition_id, err, retryable)
        end
    end
end


local function _batch_send(self, sendbuffer)
    ngx_log(WARN, "[TRACE-BATCH] Starting _batch_send, max_retry=", self.max_retry)
    local try_num = 1
    while try_num <= self.max_retry do
        ngx_log(WARN, "[TRACE-BATCH] Retry attempt ", try_num, " of ", self.max_retry)
        -- aggregator
        local send_num, sendbroker = sendbuffer:aggregator(self.client)
        ngx_log(WARN, "[TRACE-BATCH] Aggregator returned send_num=", send_num)
        if send_num == 0 then
            ngx_log(WARN, "[TRACE-BATCH] No messages to send, breaking")
            break
        end

        for i = 1, send_num, 2 do
            local broker_conf, topic_partitions = sendbroker[i], sendbroker[i + 1]
            
            -- Calculate total messages and bytes in this batch
            local total_messages = 0
            local total_bytes = 0
            local partition_count = 0
            for topic, partitions in pairs(topic_partitions.topics) do
                for partition_id, buffer in pairs(partitions.partitions) do
                    total_messages = total_messages + (buffer.index / 2)
                    total_bytes = total_bytes + buffer.size
                    partition_count = partition_count + 1
                end
            end
            
            ngx_log(WARN, "[TRACE-BATCH] Sending batch ", (i + 1) / 2, " to broker=", 
                broker_conf.host, ":", broker_conf.port,
                ", total_messages=", total_messages,
                ", total_bytes=", total_bytes,
                ", topics=", topic_partitions.topic_num,
                ", partitions=", partition_count)

            _send(self, broker_conf, topic_partitions)
        end

        local done = sendbuffer:done()
        ngx_log(WARN, "[TRACE-BATCH] After _send, sendbuffer:done()=", tostring(done))
        if done then
            ngx_log(WARN, "[TRACE-BATCH] All messages sent successfully")
            return true
        end

        ngx_log(WARN, "[TRACE-BATCH] Not all messages sent, refreshing client metadata")
        self.client:refresh()

        try_num = try_num + 1
        if try_num < self.max_retry then
            ngx_log(WARN, "[TRACE-BATCH] Sleeping for ", self.retry_backoff, "ms before retry")
            ngx_sleep(self.retry_backoff / 1000)   -- ms to s
        end
    end
    ngx_log(WARN, "[TRACE-BATCH] _batch_send completed, returning nil (not all done)")
end


local _flush_buffer


local function _flush(premature, self)
    ngx_log(WARN, "[TRACE-FLUSH] Starting _flush, premature=", tostring(premature))
    if not _flush_lock(self) then
        if debug then
            ngx_log(DEBUG, "previous flush not finished")
        end
        ngx_log(WARN, "[TRACE-FLUSH] Could not acquire flush lock, returning")
        return
    end

    local ringbuffer = self.ringbuffer
    local sendbuffer = self.sendbuffer
    local pop_count = 0
    ngx_log(WARN, "[TRACE-FLUSH] Starting to pop from ringbuffer, left_num=", ringbuffer:left_num())
    
    while true do
        local topic, key, msg = ringbuffer:pop()
        if not topic then
            ngx_log(WARN, "[TRACE-FLUSH] No more messages in ringbuffer after ", pop_count, " pops")
            break
        end
        
        pop_count = pop_count + 1
        ngx_log(WARN, "[TRACE-FLUSH] Popped message #", pop_count, 
            ": topic_type=", type(topic), ", topic=", truncate_for_log(topic, 1000),
            ", key_type=", type(key), ", key=", truncate_for_log(key, 1000),
            ", msg_type=", type(msg), ", msg=", truncate_for_log(msg, 2000))

        local partition_id, err = choose_partition(self, topic, key)
        ngx_log(WARN, "[TRACE-FLUSH] choose_partition returned partition_id=", 
            partition_id or "nil", ", err=", err or "nil")
        if not partition_id then
            partition_id = -1
            ngx_log(WARN, "[TRACE-FLUSH] Using fallback partition_id=-1")
        end

        ngx_log(WARN, "[TRACE-FLUSH] Adding to sendbuffer: topic=", truncate_for_log(topic, 1000), 
            ", partition_id=", partition_id, ", key=", truncate_for_log(key, 1000), 
            ", msg=", truncate_for_log(msg, 2000))
        local overflow = sendbuffer:add(topic, partition_id, key, msg)
        if overflow then    -- reached batch_size in one topic-partition
            ngx_log(WARN, "[TRACE-FLUSH] Sendbuffer overflow, breaking loop")
            break
        end
    end

    ngx_log(WARN, "[TRACE-FLUSH] Calling _batch_send")
    local all_done = _batch_send(self, sendbuffer)
    ngx_log(WARN, "[TRACE-FLUSH] _batch_send returned all_done=", tostring(all_done))

    if not all_done then
        ngx_log(WARN, "[TRACE-FLUSH] Not all messages sent, iterating through sendbuffer errors")
        ngx_log(WARN, "[TRACE-FLUSH] sendbuffer.queue_num=", sendbuffer.queue_num)
        
        -- Debug: dump all topics/partitions in sendbuffer before loop
        for t, partitions in pairs(sendbuffer.topics) do
            for p, buf in pairs(partitions) do
                ngx_log(WARN, "[TRACE-FLUSH-DEBUG] Pre-loop buffer state: topic=", truncate_for_log(t, 1000),
                    ", partition=", p, ", index=", buf.index, ", err=", buf.err or "nil",
                    ", retryable=", tostring(buf.retryable), ", size=", buf.size)
            end
        end
        
        local loop_iteration_count = 0
        for topic, partition_id, buffer in sendbuffer:loop() do
            loop_iteration_count = loop_iteration_count + 1
            ngx_log(WARN, "[TRACE-FLUSH] Loop iteration #", loop_iteration_count)
            local queue, index, err, retryable = buffer.queue, buffer.index, buffer.err, buffer.retryable
            
            ngx_log(WARN, "[TRACE-FLUSH] Error buffer: topic=", topic, ", partition_id=", partition_id,
                ", index=", index, ", msg_count=", index / 2, ", err=", err or "nil", 
                ", retryable=", tostring(retryable))
            
            -- Log each failed message in the buffer
            for i = 1, index, 2 do
                local failed_key = queue[i]
                local failed_msg = queue[i + 1]
                ngx_log(WARN, "[TRACE-FLUSH] Failed message[", (i + 1) / 2, "] key_type=", type(failed_key),
                    ", key=", truncate_for_log(failed_key, 1000),
                    ", msg_type=", type(failed_msg),
                    ", msg=", truncate_for_log(failed_msg, 2000))
            end

            if self.error_handle then
                ngx_log(WARN, "[TRACE-FLUSH] Calling custom error_handle callback")
                local ok, err = pcall(self.error_handle, topic, partition_id, queue, index, err, retryable)
                if not ok then
                    ngx_log(ERR, "failed to callback error_handle: ", err)
                end
            else
                ngx_log(ERR, "buffered messages send to kafka err: ", err,
                    ", retryable: ", retryable, ", topic: ", topic,
                    ", partition_id: ", partition_id, ", length: ", index / 2)
            end

            sendbuffer:clear(topic, partition_id)
        end
        ngx_log(WARN, "[TRACE-FLUSH] Loop completed, total iterations=", loop_iteration_count)
    end

    _flush_unlock(self)

    -- reset _timer_flushing_buffer after flushing complete
    self._timer_flushing_buffer = false

    if ringbuffer:need_send() then
        ngx_log(WARN, "[TRACE-FLUSH] Ringbuffer needs send, scheduling another flush")
        _flush_buffer(self)

    elseif is_exiting() and ringbuffer:left_num() > 0 then
        -- still can create 0 timer even exiting
        ngx_log(WARN, "[TRACE-FLUSH] Worker exiting but ringbuffer has messages, scheduling flush")
        _flush_buffer(self)
    end

    ngx_log(WARN, "[TRACE-FLUSH] _flush completed")
    return true
end


_flush_buffer = function (self)
    if self._timer_flushing_buffer then
        if debug then
            ngx_log(DEBUG, "another timer is flushing buffer, skipping it")
        end

        return
    end

    local ok, err = timer_at(0, _flush, self)
    if ok then
        self._timer_flushing_buffer = true
        return
    end

    ngx_log(ERR, "failed to create timer_at timer, err:", err)
end


local function _timer_flush(premature, self)
    self._timer_flushing_buffer = false
    _flush_buffer(self)
end


function _M.new(self, broker_list, producer_config, cluster_name)
    local name = cluster_name or DEFAULT_CLUSTER_NAME
    local opts = producer_config or {}
    local async = opts.producer_type == "async"
    if async and cluster_inited[name] then
        return cluster_inited[name]
    end

    local cli = client:new(broker_list, producer_config)
    local p = setmetatable({
        client = cli,
        correlation_id = 1,
        request_timeout = opts.request_timeout or 2000,
        retry_backoff = opts.retry_backoff or 100,   -- ms
        max_retry = opts.max_retry or 3,
        required_acks = opts.required_acks or 1,
        partitioner = opts.partitioner or default_partitioner,
        error_handle = opts.error_handle,
        api_version = opts.api_version or API_VERSION_V1,
        async = async,
        socket_config = cli.socket_config,
        _timer_flushing_buffer = false,
        ringbuffer = ringbuffer:new(opts.batch_num or 200, opts.max_buffering or 50000,
                opts.wait_on_buffer_full or false, opts.wait_buffer_timeout or 5),   -- 200, 50K, flase, 5s
        sendbuffer = sendbuffer:new(opts.batch_num or 200, opts.batch_size or 1048576)
                        -- default: 1K, 1M
                        -- batch_size should less than (MaxRequestSize / 2 - 10KiB)
                        -- config in the kafka server, default 100M
    }, mt)

    if async then
        cluster_inited[name] = p
        local ok, err = timer_every((opts.flush_time or 1000) / 1000, _timer_flush, p) -- default: 1s
        if not ok then
            ngx_log(ERR, "failed to create timer_every, err: ", err)
        end

    end

    return p
end


-- offset is cdata (LL in luajit)
function _M.send(self, topic, key, message)
    ngx_log(WARN, "[TRACE-ENTRY] _M.send called: topic_type=", type(topic), 
        ", topic=", truncate_for_log(topic, 1000),
        ", key_type=", type(key), ", key=", truncate_for_log(key, 1000),
        ", message_type=", type(message), ", message=", truncate_for_log(message, 200),
        ", message_len=", message and #message or 0,
        ", async=", tostring(self.async))
    
    if self.async then
        local ringbuffer = self.ringbuffer
        ngx_log(WARN, "[TRACE-ENTRY] Async mode: adding to ringbuffer, current left_num=", ringbuffer:left_num())

        local ok, err = ringbuffer:add(topic, key, message)
        ngx_log(WARN, "[TRACE-ENTRY] ringbuffer:add returned ok=", tostring(ok), ", err=", err or "nil")
        if not ok then
            ngx_log(WARN, "[TRACE-ENTRY] Failed to add to ringbuffer: ", err)
            return nil, err
        end

        ngx_log(WARN, "[TRACE-ENTRY] After add: left_num=", ringbuffer:left_num(), 
            ", need_send=", tostring(ringbuffer:need_send()),
            ", flushing=", tostring(self.flushing), ", is_exiting=", tostring(is_exiting()))
        
        if not self.flushing and (ringbuffer:need_send() or is_exiting()) then
            ngx_log(WARN, "[TRACE-ENTRY] Triggering _flush_buffer")
            _flush_buffer(self)
        end

        return true
    end

    ngx_log(WARN, "[TRACE-ENTRY] Sync mode: choosing partition")
    local partition_id, err = choose_partition(self, topic, key)
    ngx_log(WARN, "[TRACE-ENTRY] choose_partition returned partition_id=", 
        partition_id or "nil", ", err=", err or "nil")
    if not partition_id then
        ngx_log(WARN, "[TRACE-ENTRY] Failed to choose partition: ", err)
        return nil, err
    end

    local sendbuffer = self.sendbuffer
    ngx_log(WARN, "[TRACE-ENTRY] Adding to sendbuffer: topic=", truncate_for_log(topic, 100), 
        ", partition_id=", partition_id)
    sendbuffer:add(topic, partition_id, key, message)

    ngx_log(WARN, "[TRACE-ENTRY] Calling _batch_send")
    local ok = _batch_send(self, sendbuffer)
    ngx_log(WARN, "[TRACE-ENTRY] _batch_send returned ok=", tostring(ok))
    if not ok then
        local send_err, send_retryable = sendbuffer:err(topic, partition_id)
        ngx_log(WARN, "[TRACE-ENTRY] Send failed, clearing buffer. err=", send_err or "nil",
            ", retryable=", tostring(send_retryable))
        sendbuffer:clear(topic, partition_id)
        return nil, send_err
    end

    local offset = sendbuffer:offset(topic, partition_id)
    ngx_log(WARN, "[TRACE-ENTRY] Send successful, offset=", tostring(offset))
    return offset
end


function _M.flush(self)
    return _flush(nil, self)
end


-- offset is cdata (LL in luajit)
function _M.offset(self)
    local topics = self.sendbuffer.topics
    local sum, details = 0, {}

    for topic, partitions in pairs(topics) do
        details[topic] = {}
        for partition_id, buffer in pairs(partitions) do
            sum = sum + buffer.offset
            details[topic][partition_id] = buffer.offset
        end
    end

    return sum, details
end


return _M
