-- UNIVARIATE TIMESERIES IN REDIS
--
-- Stand-alone Lua script for managing an univariate timeseries in Redis
--
-- A timeseries is an
--  1) ordered (with respect to timestamps)
--  2) unique (each timestamp is unique within the timeseries)
--  3) associative (it associate a timestamp with a value)
-- container.
-- Commands are implemented in the *Timeseries* table. To
-- execute a command use EVALSHA as follows
--
--  EVALSHA sha1 1 key add 100000 data
--  EVALSHA sha1 1 key range 100000 100500 0 25

local _NAME = 'timeseries.lua'
local _VERSION = '0.0.1'
local _DESCRIPTION = 'A library for simple timeseries handling in Redis'
local _COPYRIGHT = '2019 Clayton Collie, Guanima Tech Labs'

local rcall = redis.call

local function ts_debug(msg)
    redis.call('rpush', 'ts-debug', msg)
end

----- TESTING ONLY - REMOVE WHEN DONE

function table.val_to_str ( v )
    if "string" == type( v ) then
        v = string.gsub( v, "\n", "\\n" )
        if string.match( string.gsub(v,"[^'\"]",""), '^"+$' ) then
            return "'" .. v .. "'"
        end
        return '"' .. string.gsub(v,'"', '\\"' ) .. '"'
    else
        return "table" == type( v ) and table.tostring( v ) or
                tostring( v )
    end
end

function table.key_to_str ( k )
    if "string" == type( k ) and string.match( k, "^[_%a][_%a%d]*$" ) then
        return k
    else
        return "[" .. table.val_to_str( k ) .. "]"
    end
end

function table.tostring( tbl )
    local result, done = {}, {}
    for k, v in ipairs( tbl ) do
        table.insert( result, table.val_to_str( v ) )
        done[ k ] = true
    end
    for k, v in pairs( tbl ) do
        if not done[ k ] then
            table.insert( result,
                    table.key_to_str( k ) .. "=" .. table.val_to_str( v ) )
        end
    end
    return "{" .. table.concat( result, "," ) .. "}"
end

--- STATS ------

local stats = {}

-- Get the mean value of a table

function stats.mean(t)
    local sum = 0
    local count = 0

    for k, v in pairs(t) do
        if type(v) == 'number' then
            sum = sum + v
            count = count + 1
        end
    end

    return (sum / count)
end

function stats.stdDeviation(t)
    local count, sum = 0, 0
    local vk, mean = 0, 0
    local std = 0
    local sqrt = math.sqrt

    for _, v in pairs(t) do
        local val = tonumber(v)
        if val ~= nil then
            local oldmean = mean
            count = count + 1
            sum = sum + val
            mean = sum / count
            vk = vk + (val - mean) * (val - oldmean)
            std = sqrt(vk / (count - 1))
        end
    end
    return std
end

-- Get the median of a table.
function stats.median( t )
    local temp={}

    -- deep copy table so that when we sort it, the original is unchanged
    -- also weed out any non numbers
    for k,v in pairs(t) do
        if type(v) == 'number' then
            table.insert( temp, v )
        end
    end

    table.sort( temp )

    -- If we have an even number of table elements or odd.
    if math.fmod(#temp,2) == 0 then
        -- return mean value of middle two elements
        return ( temp[#temp/2] + temp[(#temp/2)+1] ) / 2
    else
        -- return middle element
        return temp[math.ceil(#temp/2)]
    end
end

function stats.basic(t)
    local count, sum = 0, 0
    local max = -math.huge
    local min = math.huge
    local vk, mean = 0, 0
    local std = 0
    local math_max = math.max
    local math_min = math.min
    local sqrt = math.sqrt

    for _, v in pairs(t) do
        local val = tonumber(v)
        if val ~= nil then
            local oldmean = mean
            count = count + 1
            sum = sum + val
            max = math_max(max, val)
            min = math_min(min, val)
            mean = sum / count
            vk = vk + (val - mean) * (val - oldmean)
            std = sqrt(vk / (count - 1))
        end
    end
    return {
        count = count,
        sum = sum,
        min = min,
        max = max,
        mean = mean,
        std = std
    }
end

--- CONSTS -----

local AGGREGATION_TYPES = {
    count = 1,
    sum = 1,
    avg = 1,
    median = 1,
    stdev = 1,
    min = 1,
    max = 1,
    first = 1,
    last = 1,
    range = 1,
    rate = 1
}

local ALL_OPTIONS = {
    LIMIT = 1,
    AGGREGATION = 1,
    FILTER = 1,
    LABELS = 1,
    REDACT = 1,
    FORMAT = 1,
    STORAGE = 1
}

local PARAMETER_OPTIONS = {
    LIMIT = 1,
    AGGREGATION = 1,
    FILTER = 1,
    LABELS = 1,
    REDACT = 1,
    FORMAT = 1
}

local COPY_OPTIONS = {
    LIMIT = 1,
    AGGREGATION = 1,
    FILTER = 1,
    LABELS = 1,
    REDACT = 1,
    STORAGE = 1
}

local FORMAT_VALUES = {
    json = 1,
    msgpack = 1
}

local STORAGE_VALUES = {
    timeseries = 1,
    hash = 1
}

--- UTILS ------

local SEPARATOR = '-'
local IDENTIFIER_PATTERN = "[%a_]+[%a%d_]*"
local ID_CAPTURE_PATTERN = '(' .. IDENTIFIER_PATTERN .. ')'
local ROLLUP_PATTERN = ID_CAPTURE_PATTERN .. '%s*%(%s*' .. ID_CAPTURE_PATTERN .. '%s*%)'

function table.extend(self, other)
    for i, v in ipairs(other) do
        table.insert(self, v)
    end
end

--- split id into timestamp and sequence number
local function split(source, sep)
    if (source == '*') then
        return source, nil
    end
    local start, ending = string.find(source, sep or SEPARATOR, 1, true)
    if (start == nil) then
        return tonumber(source), nil
    end
    local timestamp = source:sub(1, start - 1)
    local sequence = source:sub(ending + 1)
    return tonumber(timestamp), sequence
end

local function is_possibly_number(val)
    local res = tonumber(val)
    local is_num = (res ~= nil)
    if is_num then val = res end
    return is_num, val
end

local function possibly_convert_float(val)
    if (type(val) == 'number' and (math.floor(val) ~= val)) then
        return tostring(val)
    end
    return val
end

local function parse_input(val)
    local is_num, num = is_possibly_number(val)
    if (is_num) then
        if (math.floor(num) ~= num) then
            return val
        end
        return num
    end
    if (val == 'true') then
        return true
    elseif (val == 'false') then
        return false
    end
    return val
end

-- not very sophisticated since our values are simple
local function to_bulk_reply(val)
    local type = type(val)
    if type == 'number' then
        -- handle floats
        if (math.floor(val) ~= val) then
            return tostring(val)
        end
        return val
    elseif type == "table" then
        local data = {}
        -- check if is_array
        if val[1] ~= nil then
            for j, xval in ipairs(val) do
                data[j] = to_bulk_reply(xval)
            end
            return data
        end
        -- associative
        local i = 1
        for k, v in pairs(val) do
            data[i] = k
            data[i + 1] = to_bulk_reply(v)
            i = i + 2
        end
        return data
    elseif type == "nil" then
        return "nil"
    end
    return val
end

local function from_hash(table)
    local i, data = 1, {}
    for k, v in pairs(table) do
        data[i] = k
        data[i + 1] = v
        i = i + 2
    end
    return data
end

-- raw xrange value should be a kv table [name, value, name, value]
-- convert to an associative array
local function to_hash(value)
    local len, result = #value, {}

    for k = 1, len, 2 do
        result[ value[k] ] = value[k+1]
    end
    return result
end

--- PARAMETER PARSING --------

local function parse_range_value(candidate, name)
    assert(candidate, 'value expected for ' .. name)
    if (candidate == '+') or (candidate == '-') then
        return candidate, true
    else
        -- todo: split between timestamp and sequence parts
        local value = split(candidate, '-')
        if (value == nil) then
            error('number expected for ' ..name)
        end
        return value, false
    end
end

local function parse_range_min_max(result, timestamp1, timestamp2)
    result[#result + 1] = parse_range_value(timestamp1, 'min')
    result[#result + 1] = parse_range_value(timestamp2, 'max')
end

local function get_key_val_varargs(method, ...)
    local arg = { ... }
    local n = #arg

    assert(n, 'No values specified for  ' .. method .. '.')
    assert(math.mod(n, 2) == 0, 'Invalid args to ' .. method .. '. Number of arguments must be even')
    return arg
end


-- Source https://help.interfaceware.com/kb/parsing-csv-files
local function parse_list (line,sep)
    local res = {}
    local pos = 1
    sep = sep or ','
    while true do
        local c = string.sub(line,pos,pos)
        if (c == "") then break end
        local posn = pos
        local ctest = string.sub(line,pos,pos)
        while ctest == ' ' do
            -- handle space(s) at the start of the line (with quoted values)
            posn = posn + 1
            ctest = string.sub(line,posn,posn)
            if ctest == '"' then
                pos = posn
                c = ctest
            end
        end
        if (c == '"') then
            -- quoted value (ignore separator within)
            local txt = ""
            repeat
                local startp,endp = string.find(line,'^%b""',pos)
                txt = txt..string.sub(line,startp+1,endp-1)
                pos = endp + 1
                c = string.sub(line,pos,pos)
                if (c == '"') then
                    txt = txt..'"'
                    -- check first char AFTER quoted string, if it is another
                    -- quoted string without separator, then append it
                    -- this is the way to "escape" the quote char in a quote. example:
                    --   value1,"blub""blip""boing",value3  will result in blub"blip"boing  for the middle
                elseif c == ' ' then
                    -- handle space(s) before the delimiter (with quoted values)
                    while c == ' ' do
                        pos = pos + 1
                        c = string.sub(line,pos,pos)
                    end
                end
            until (c ~= '"')
            table.insert(res,txt)
            -- trace(c,pos,i)
            if not (c == sep or c == "") then
                error("ERROR: Invalid field - near character "..pos.." in the item list: \n"..line, 3)
            end
            pos = pos + 1
            posn = pos
            ctest = string.sub(line,pos,pos)
            -- trace(ctest)
            while ctest == ' ' do
                -- handle space(s) after the delimiter (with quoted values)
                posn = posn + 1
                ctest = string.sub(line,posn,posn)
                if ctest == '"' then
                    pos = posn
                    c = ctest
                end
            end
        else
            -- no quotes used, just look for the first separator
            local startp, endp = string.find(line,sep,pos)
            if (startp) then
                table.insert(res,string.sub(line,pos,startp-1))
                pos = endp + 1
            else
                -- no separator found -> use rest of string and terminate
                table.insert(res,string.sub(line,pos))
                break
            end
        end
    end
    return res
end

--- Parse a filter condition and return a function implementing
--- the corresponding filter predicate
local function parse_filter_condition(exp)

    local function compare(_field, _op, _val)
        local num = tonumber(_val)
        local is_numeric = num ~= nil
        if (is_numeric) then
            _val = num
        else
            _val = parse_input(_val)
        end
        local val_type = type(_val)
        return function(v)
            local val_type = val_type
            local val = _val
            local is_numeric = is_numeric
            local op = _op
            local field = _field

            local to_compare = v[field]

            -- ts_debug('compare: ' .. tostring(val) .. tostring(op) .. tostring(to_compare))

            if (val_type ~= type(to_compare)) then
                if is_numeric then
                    to_compare = tonumber(to_compare)
                else
                    -- convert all to string
                    val = tostring(val)
                    to_compare = tostring(to_compare)
                end
            end
            -- ts_debug('filtering. v = ' .. table.tostring(v) .. ' field = ' .. field .. ', comp = ' .. tostring(to_compare) .. ' ' .. op .. ' ' .. tostring(val))
            if (op == 'eq') then
                return val == to_compare
            elseif (op == 'ne') then
                return val ~= to_compare
            else
                if (val == nil) or (to_compare == nil) then
                    return false
                end
                if (op == 'gt') then
                    return to_compare > val
                elseif (op == 'lt') then
                    return to_compare < val
                elseif (op == 'gte') then
                    return to_compare >= val
                elseif (op == 'lte') then
                    return to_compare <= val
                end
            end
            return false
        end
    end

    local function contains(_field, _matches)
        return function(v)
            local matches = _matches
            local field = _field
            return matches[tostring(v[field])] ~= nil
        end
    end

    local function not_contains(_field, _matches)
        return function(v)
            local matches = _matches
            local field = _field
            return matches[tostring(v[field])] == nil
        end
    end

    local function match_contains(cond)
        local values, _, field
        local ops = { '!=', '=' }
        local contains_funcs = { not_contains, contains }

        for i, op in ipairs(ops) do
            local pattern = ID_CAPTURE_PATTERN .. op .. '(%b())'
            _, _, field, values = string.find(cond, pattern)
            if field and values then
                -- remove parens
                values = values:sub(2, values:len() - 1)

                local matches = parse_list(values, ',')
                if #matches == 0 then
                    error('No values found for contains match')
                end
                -- transform indexed array to an associative hash for faster comparisons
                local temp = {}
                for _, val in ipairs(matches) do
                    temp[tostring(val)] = 1
                end
                matches = temp
                return contains_funcs[i](field, matches)
            end
        end

        return nil
    end

    local function match_ops(cond)
        local pattern, val, field, _
        local ops = { '!=', '<=', '>=', '=', '<', '>' }
        local op_names = { 'ne', 'lte', 'gte', 'eq', 'lt', 'gt' }

        for i, op in ipairs(ops) do
            pattern = ID_CAPTURE_PATTERN .. op .. '(.+)'
            _, _, field, val = string.find(cond, pattern)
            if (field and val) then
                return compare(field, op_names[i], val)
            end
        end

        return nil
    end

    local p = assert(match_contains(exp) or match_ops(exp), 'FILTER: unable to parse expression : ' .. exp)

    return p
end

-- Returns a predicate function that matches
-- *all* of the given predicate functions.
local function join_AND(predicates)
    if (#predicates == 1) then
        return predicates[1]
    end
    return function(s)
        local preds = predicates
        for _, func in ipairs(preds) do
            if not func(s) then
                return false
            end
        end
        return true
    end
end

-- Returns a predicate function that matches
-- *any* of the given predicate functions.
local function join_OR(predicates)
    if (#predicates == 1) then
        return predicates[1]
    end
    return function(s)
        local preds = predicates
        for _, func in ipairs(preds) do
            if func(s) then
                return true
            end
        end
        return false
    end
end

local function parse_filter(args, i)
    local predicate, expr
    local predicates = {}
    local count = 0
    local u_expr
    local len = #args
    local join_funcs = {
        OR = join_OR,
        AND = join_AND
    }

    local function parse_join(chain, arg, op, j)
        local exp = op;
        while (exp == op) do
            j = j + 1
            chain[#chain + 1] = parse_filter_condition(arg[j])
            j = j + 1
            if (j >= len) then
                break
            end
            exp = string.upper(arg[j])
        end
        return j
    end

    local chain
    local parse_filter_condition = parse_filter_condition
    while i <= len do
        expr = args[i]
        u_expr = string.upper(expr)
        if (ALL_OPTIONS[u_expr]) then
            break
        end

        predicate = parse_filter_condition(expr)
        i = i + 1
        u_expr = string.upper(args[i] or '')
        while (u_expr == 'AND') or (u_expr == 'OR') do
            chain = { predicate }
            i = parse_join(chain, args, u_expr, i)
            predicate = join_funcs[u_expr](chain)
            if (i >= len) then
                break
            end
            u_expr = string.upper(args[i])
        end
        count = count + 1
        predicates[count] = predicate
    end

    assert(count > 0, 'FILTER: at least one condition must be specified ')
    -- construct final predicate
    -- Optimize for common case (1 condition)
    return join_AND(predicates), i
end

local function parse_range_params(valid_options, min, max, ...)
    local fetch_params = {}
    parse_range_min_max(fetch_params, min, max)

    local result = {
        min = fetch_params[1],
        max = fetch_params[2]
    }

    valid_options = valid_options or PARAMETER_OPTIONS

    local arg = { ... }
    local i = 1

    --- ts_debug('args = ' .. table.tostring(arg))
    --- [LIMIT count] or
    --- [AGGREGATION bucketWidth aggregateType]
    --- [FILTER key=value, ...]
    --- [LABELS name1, name2 ....]
    while i < #arg do
        local option_name = assert(arg[i], 'range: no option specified')
        option_name = string.upper(option_name)

        if (not valid_options[option_name]) then
            local j = 0
            local str = ''
            for k, _ in pairs(valid_options) do
                if str:len() > 0 then
                    str = str .. ', '
                end
                str = str .. k
                j = j + 1
            end
            error('Invalid option "' .. option_name .. '". Expected one of ' .. str)
        end

        i = i + 1
        if (option_name == 'LIMIT') then
            assert(not result.count, 'A value for limit has already been set')

            -- we should have offset, count
            local count = assert( tonumber(arg[i]), 'LIMIT: value must be a number')
            assert(count >= 0, "LIMIT: value must be positive")
            result.count = count
            i = i + 2
        elseif (option_name == 'AGGREGATION') then
            assert(not result.aggregate, 'A value for aggregate has already been set')

            local bucketSize = arg[i]
            result.labels = {}
            result.aggregate = {
                timeBucket = assert(tonumber(bucketSize), 'AGGREGATE: timeBucket must be a number. Got "' .. bucketSize .. '"'),
                fields = {}
            }
            i = i + 1
            while i <= #arg do
                local agg = arg[i]
                if (ALL_OPTIONS[string.upper(agg)]) then
                    break
                end
                local rollup, field = string.match(agg, ROLLUP_PATTERN)
                assert(field, 'invalid or missing aggregate spec ' .. '"' .. (agg or '') .. '"')
                rollup = assert(string.lower(rollup), 'missing value for aggregate type')
                assert(AGGREGATION_TYPES[rollup], 'invalid aggregation type : "' .. rollup ..'"')
                table.insert(result.aggregate.fields, { field, rollup })
                result.labels[field] = 1
                i = i + 1
            end
            -- make sure some fields were specified
            assert(#result.aggregate.fields, 'No fields specified for aggregation')
        elseif (option_name == "LABELS") then
            assert(not result.redacted, 'Either specify REDACT or LABELS, but not both')
            result.labels = {}
            while i <= #arg do
                local key = arg[i]
                if (PARAMETER_OPTIONS[ string.upper(key) ]) then
                    break
                end
                result.labels[key] = 1
                i = i + 1
            end
        elseif (option_name == 'REDACT') then
            assert(not result.redacted, 'REDACT option already specified')
            assert(not result.labels, 'Either specify REDACT or LABELS, but not both')
            result.redacted = {}
            while i <= #arg do
                local key = arg[i]
                if (PARAMETER_OPTIONS[ string.upper(key) ]) then
                    break
                end
                result.redacted[key] = 1
                i = i + 1
            end
        elseif (option_name == 'FILTER') then
            assert(not result.filter, 'FILTER conditions already set')
            local predicate

            predicate, i = parse_filter(arg, i)

            result.filter = predicate
        elseif (option_name == 'STORAGE') then
            assert(not result.storage, 'STORAGE already set')
            local storage = string.lower(arg[i] or '')
            assert(STORAGE_VALUES[storage], 'STORAGE: Expecting "timeseries" or "hash", got "' .. storage .. '"')
            result.storage = storage
            i = i + 1
        end
    end

    result.should_pick = result.labels or result.redacted
    result.parse_ts = result.aggregate ~= nil
    return result
end

-- value is raw value from xrange
-- only call this if (options.labels or options.redacted)
local function pick(value, options)
    local hash = {}
    local key, val, valid
    local i, len = 1, #value

    for k = 1, len, 2 do
        key = value[k]
        val = value[k+1]
        valid = true
        if options.labels then
            valid = options.labels[key]
        elseif options.redacted then
            valid = (not options.redacted[key])
        end
        if valid then
            hash[ i ] = key
            hash[ i + 1 ] = val
            -- hash[ val[k] ] = is_possibly_number(val[k+1])
            i = i + 2
        end
    end
    return hash
end

local function process_range(range, options)
    local result = {}
    local valid
    local ts, val, entry
    local decode = to_hash
    local split = split
    local pick = pick
    options = options or {}
    local should_pick = options.labels or options.redacted

    local filter = options.filter
    local i = 1
    for _, value in ipairs(range) do
        ts = value[1]
        val = value[2]

        valid = true
        if (filter) then
            local hash = decode(val)
            valid = filter(hash)
        end
        if valid then
            if should_pick then
                val = pick(val, options)
            end
            entry = {ts, val}
            if (options.parse_ts) then
                entry[3], entry[4] = split(ts, '-')
            end
            result[i] = entry
            i = i + 1
        end
    end
    return result
end

local function get_single_value(key, timestamp, options, name)
    local ra = redis.call('XRANGE', key, timestamp, timestamp, 'COUNT', 2)
    options = options or {}
    if ra ~= nil and #ra == 1 then
        local entry = ra[1]
        local should_pick = options.labels or options.redacted
        local value = (should_pick and pick(entry[2], options)) or entry[2]
        return entry[1], value
    elseif #ra > 1 then
        error('Critical error in timeseries.' .. name .. ' : multiple values for a timestamp')
    end
    return nil, nil
end

local function agg_collect_numbers(result, key, val)
    val = tonumber(val)
    if val ~= nil then
        result[key] = result[key] or {}
        table.insert(result[key], val)
    end
end

local AGGR_ITERATION_FUNCS = {
    count = function(result, key, val)
        result[key] = tonumber(result[key] or 0) + 1
    end,
    sum = function(result, key, val)
        val = tonumber(val) or 0
        result[key] = tonumber(result[key] or 0) + val
    end,
    avg = agg_collect_numbers,
    median = agg_collect_numbers,
    stdev = agg_collect_numbers,
    rate = function(result, key, val)
        result[key] = tonumber(result[key] or 0) + 1
    end,
    min = function(result, key, val)
        local is_number
        local current = result[key]
        if (val ~= nil) then
            is_number, val = is_possibly_number(val)
            if (is_number) then
                result[key] = math.min(current or math.huge, val)
            else
                current = current or ''
                if (val < current) then
                    result[key] = val
                else
                    result[key] = current
                end
            end
        end
    end,
    max = function(result, key, val)
        local is_number
        local current = result[key]
        if (val ~= nil) then
            is_number, val = is_possibly_number(val)
            if (is_number) then
                result[key] = math.max(current or -math.huge, val)
            else
                current = current or ''
                if (val > current) then
                    result[key] = val
                else
                    result[key] = current
                end
            end
        end
    end,
    range = function(result, key, val)
        val = tonumber(val)
        if val ~= nil then
            result[key] = result[key] or { min = math.huge, max = -math.huge }
            local min_max = result[key]
            min_max.min = math.min(min_max.min, val)
            min_max.max = math.max(min_max.max, val)
        end
    end,
    first = function(result, key, val)
        if (val ~= nil) then
            if (result[key] == nil) then
                result[key] = val
            end
        end
    end,
    last = function(result, key, val)
        result[key] = val
    end
}

local AGGR_FINALIZE_FUNCS = {
    default = function(result)
        for bucket, data in pairs(result) do
            result[bucket] = possibly_convert_float( data )
        end
        return result
    end,
    avg = function(result)
        for bucket, data in pairs(result) do
            result[bucket] = possibly_convert_float( stats.mean(data) )
        end
        return result
    end,
    median = function(result)
        for bucket, data in pairs(result) do
            result[bucket] = possibly_convert_float( stats.median(data) )
        end
        return result
    end,
    stdev = function(result)
        for bucket, data in pairs(result) do
            result[bucket] = possibly_convert_float( stats.stdDeviation(data) )
        end
        return result
    end,
    stats = function(result)
        for bucket, data in pairs(result) do
            local stat = stats.basic(data)
            local temp = {}
            for k, v in pairs(stat) do
                temp[#temp + 1] = k
                temp[#temp + 1] = possibly_convert_float(v)
            end
            result[bucket] = temp
        end
        return result
    end,
    range = function(result)
        for bucket, min_max in pairs(result) do
            if (min_max ~= nil) then
                result[bucket] = possibly_convert_float(min_max.max - min_max.min)
            else
                result[bucket] = false  -- how do we return nil back to redis ????
            end
        end
        return result
    end,
    rate = function(result, timeBucket)
        for bucket, count in pairs(result) do
            result[bucket] = tostring(count / timeBucket)
        end
        return result
    end
}

local function aggregate(range, aggregationType, timeBucket)
    local result = {}
    local ts, key, val

    local iterate = assert(AGGR_ITERATION_FUNCS[aggregationType], 'invalid aggregate type "' .. tostring(aggregationType) .. '"')
    local finalize = AGGR_FINALIZE_FUNCS[aggregationType] or AGGR_FINALIZE_FUNCS.default

    for _, kv in ipairs(range) do
        ts = kv[1] - (kv[1] % timeBucket)
        val = kv[2]
        key = tostring(ts)
        iterate(result, key, val)
    end

    return finalize(result, timeBucket)
end

local function base_range(cmd, key, params)
    local fetch_params = {key, params.min, params.max}
    if (params.count ~= nil) then
        fetch_params[#fetch_params + 1] = 'COUNT'
        fetch_params[#fetch_params + 1] = params.count
    end
    return redis.call(cmd, unpack(fetch_params))
end

-- COMMANDS TABLE
local Timeseries = {
}

Timeseries.__index = Timeseries;

-- Add timestamp-value pairs to the Timeseries
function Timeseries.add(key, timestamp, ...)
    local args = { ... }
    local n = #args

    assert(n, 'No values specified for  "add"')
    assert( math.mod(n, 2) == 0 , 'Invalid args to add. Number of arguments must be even')

    return redis.call('XADD', key, timestamp, unpack(args))
end

function Timeseries.bulkAdd(key, ...)
    local values = get_key_val_varargs('bulkAdd', ...)
    local len = #values
    local count = 0

    for i = 1, len, 2 do
        local ts = values[i]
        -- should be a json encoded string
        local val = cjson.decode(values[i + 1])
        local args = from_hash(val)
        redis.call('XADD', key, ts, unpack(args))
        count = count + 1
    end

    return count
end

function Timeseries.del(key, ...)
    local args = { ... }
    assert(#args > 0, "At least one item must be specified for del")
    return redis.call('XDEL', key, unpack(args))
end

function Timeseries.size(key)
    if not redis.call('exists', key) then
        return false
    end
    return redis.call('XLEN', key)
end

-- Count the number of elements between *min* and *max*
function Timeseries.count(key, min, max, ...)
    local params = parse_range_params({ FILTER = 1 }, min, max, ...)
    local data = base_range('XRANGE', key, params)

    if (data == nil) or (#data == 0) then
        return 0
    end

    if (params.filter == nil) then
        return #data
    end

    local values = process_range(data, params)
    return #values
end

-- Check if *timestamp* exists in the timeseries
function Timeseries.exists(key, timestamp)
    local ts, value = get_single_value(key, timestamp, {}, 'exists')
    if ts ~= nil then
        return 1
    else
        return 0
    end
end

function Timeseries.info(key)
    if rcall('exists', key) == 0 then return nil end
    local info = redis.pcall('XINFO', 'STREAM', key)
    return info
end

function Timeseries.span(key)
    if redis.call('exists', key) == 0 then return nil end
    local info = redis.pcall('XINFO', 'STREAM', key)
    if (info ~= nil) then
        local first, last
        for i, v in ipairs(info) do
            if (v == 'first-entry') then first = info[i+1] end
            if (v == 'last-entry') then last = info[i+1] end
            if (first and last) then break end
        end
        if (first ~= nil) and #first then
            first = first[1]
        else
            first = false
        end
        if (last ~= nil) and #last then
            last = last[1]
        else
            last = false
        end
        return { first, last }
    end
    return nil
end

function Timeseries._get(remove, key, timestamp, ...)
    local params = parse_range_params({ LABELS = 1, REDACT = 1, FORMAT = 1 }, timestamp, timestamp, ...)
    local ts, value = get_single_value(key, timestamp, params, 'get')
    if ts then
        if (remove) then
            redis.call("XDEL", key, ts)
        end
        if (params.format == 'json') then
            if not (params.should_pick) then
                -- not a hash
                value = to_hash(value)
            end
            return cjson.encode(value)
        end
    end
    return value
end

-- Get the value associated with *timestamp*, optionally selecting only a given set of labels
function Timeseries.get(key, timestamp, ...)
    return Timeseries._get(false, key, timestamp, ...)
end

-- Remove and return the value associated with *timestamp*
function Timeseries.pop(key, timestamp, ...)
    return Timeseries._get(true, key, timestamp, ...)
end

local function remove_values(key, values, filter)
    if values and #values > 0 then

        local ids = {}
        if (filter ~= nil) then
            local params = {
                filter = filter
            }
            values = process_range(values, params)
        end
        for i, value in ipairs(values) do
            ids[i] = value[1]
        end

        return redis.call('XDEL', key, unpack(ids))
    end
    return 0
end

-- Remove and return a range between *min* and *max*
function Timeseries.poprange(key, min, max, ...)
    return Timeseries._range(true, "XRANGE", key, min, max, ...)
end

-- Remove a range between *min* and *max*
function Timeseries.remrange(key, min, max, ...)
    local range = Timeseries._range(true, "XRANGE", key, min, max, ...)
    return #range
end

function Timeseries._aggregate(range, agg_params)
    local aggregate = aggregate
    local by_key = {}
    local k
    for _, v in ipairs(range) do
        local ts = v[3]
        local hash = v[2] or {}
        for i = 1, #hash, 2 do
            k = hash[i]
            by_key[k] = by_key[k] or {}
            table.insert(by_key[k], { ts, hash[i + 1] })
        end
    end
    local result = {}
    local timestamps = {}
    local bucket_hash = {}

    for _, field_info in ipairs(agg_params.fields) do
        local key = field_info[1]
        local agg_type = field_info[2]
        local values = by_key[key]

        if (values and #values > 0) then
            local buckets = aggregate(values, agg_type, agg_params.timeBucket)
            for k, value in pairs(buckets) do
                result[k] = result[k] or {}
                result[k][key] = result[k][key] or {}

                local temp = result[k][key]
                temp[#temp + 1] = agg_type
                temp[#temp + 1] = value

                if (bucket_hash[k] == nil) then
                    bucket_hash[k] = 1
                    timestamps[#timestamps + 1] = { tonumber(k), k }
                end
            end
        end
    end

    -- use timestamps to transform hash into properly ordered indexed array
    table.sort(timestamps, function(a, b) return a[1] < b[1] end)

    local final = {
        timestamps = timestamps,
        data = result
    }
    return final
end

function Timeseries._range(remove, cmd, key, min, max, ...)

    local function handle_aggregation(range, agg_params, format)
        local agg_response = Timeseries._aggregate(range, agg_params, format)

        local timestamps = agg_response.timestamps
        local result = agg_response.data

        local final = {}
        if (format == 'json') then
            for i, ts in ipairs(timestamps) do
                final[i] = { ts[1], result[ts[2]] }
            end
            final = cjson.encode(final)
        else
            local k = 1
            local val
            for _, ts in ipairs(timestamps) do
                val = result[ts[2]]
                final[k] = ts[1]
                final[k + 1] = to_bulk_reply(val)
                k = k + 2
            end
        end

        return final
    end

    local options = PARAMETER_OPTIONS
    local params = parse_range_params(options, min, max, ...)

    local data = base_range(cmd, key, params)

    if data and #data > 0 then
        local range = process_range(data, params)

        if (remove) then
            remove_values(key, range)
        end

        if params.aggregate ~= nil then
            range = handle_aggregation(range, params.aggregate, params.format)
        end
        return range
    end

    return {}
end

-- The list of timestamp-value pairs between *timestamp1* and *max*
function Timeseries.range(key, min, max, ...)
    return Timeseries._range(false,'XRANGE', key, min, max, ...)
end

function Timeseries.revrange(key, min, max, ...)
    return Timeseries._range(false, 'XREVRANGE', key, min, max, ...)
end

function Timeseries.trimlength(key, count, approximate)
    approximate = approximate ~= nil and approximate ~= 0
    local args = {"MAXLEN"}
    if approximate then
        args[#args + 1] = "~"
    end
    args[#args + 1] = count
    return redis.call('XTRIM', key, unpack(args))
end

-- list of timestamps between *min* and *max*
function Timeseries.times(key, min, max)
    min = min or '-'
    max = max or '+'
    local range = Timeseries.range(key, min, max)
    local result = {}
    for _, v in ipairs(range) do
        table.insert(result, v[1])
    end
    return result
end

local function storeHash(dest, range, params)
    local args = {}
    for _, val in ipairs(range) do
        local ts = val[1]
        local data = val[2]

        if type(data) == 'table' then
            if (params.should_pick) then
                -- its an associative array
                data = cjson.encode( data )
            else
                data = cjson.encode( to_hash(data) )
            end
        end
        args[#args + 1] = tostring(ts)
        args[#args + 1] = data
    end
    redis.call('hmset', dest, unpack(args))
end

local function storeTimeseries(dest, range, params)
    for _, val in ipairs(range) do
        local ts = val[1]
        local data = val[2]
        if type(data) ~= 'table' then
            data = {'value', data}
        end
        Timeseries.add(dest, ts, unpack(data))
    end
end

local function storeResult(dest, range, params)
    local storage = params.storage or 'timeseries'
    if (#range) then
        if storage == 'timeseries' then
            storeTimeseries(dest, range, params)
        else
            storeHash(dest, range, params)
        end
    end

    return #range
end

--- copy data from a timeseries and store it in another key
function Timeseries.copy(key, dest, min, max, ...)

    -- convert hash in form returned from aggregation to a version compatible with XADD/HMSET depending on
    -- STORAGE option
    local function transform_value(val, agg_params, is_timeseries)
        local result = {}
        for _, field_info in ipairs(agg_params.fields) do
            local key = field_info[1]
            local values = val[key]

            local sep = '_'
            if (values and #values > 0) then
                for j = 1, #values, 2 do
                    local slot_key = key .. sep .. values[j]
                    if (is_timeseries) then
                        result[#result + 1] = slot_key
                        result[#result + 1] = values[j + 1]
                    else
                        result[slot_key] = values[j + 1]
                    end
                end
            end

        end
        return result
    end

    local function handle_aggregation(range, agg_params, is_timeseries)
        local agg_response = Timeseries._aggregate(range, agg_params)
        local timestamps = agg_response.timestamps
        local result = agg_response.data

        local final = {}
        local val, timestamp
        for i, ts in ipairs(timestamps) do
            timestamp = ts[1]
            val = result[ts[2]]
            val = transform_value( val, agg_params, is_timeseries )
            final[i] = { timestamp, val }
        end

        return final
    end

    local params = parse_range_params(COPY_OPTIONS, min, max, ...)
    local data = base_range('XRANGE', key, params)
    local is_timeseries = (not params.storage) or (params.storage == 'timeseries')

    if (#data == 0) then
        return 0
    end

    local has_aggregation = params.aggregate ~= nil
    local has_filter = params.filter ~= nil

    -- Fast path if no filtering or transformation
    if ((not has_filter) and (not has_aggregation) and is_timeseries) and (not params.should_pick)  then
        for _, val in ipairs(data) do
            local ts = val[1]
            local args = val[2]
            redis.call('XADD', dest, ts, unpack(args))
        end
        return #data
    end

    local range = process_range(data, params)
    if params.aggregate ~= nil then
        range = handle_aggregation(range, params.aggregate, is_timeseries)
    end

    return storeResult(dest, range, params)
end

--- Merge the data from 2 timeseries and store it in another key
function Timeseries.merge(firstKey, secondKey, dest, min, max, ...)

    local function emit(ts, value, params)
        -- see if we are a hash, convert back
        if (params.should_pick) then
            value = from_hash(value)
        end

        redis.call("XADD", dest, ts, unpack(value))
    end

    local function add_remaining(array, start)
        local emit = emit
        local len = #array
        local id, val
        while start <= len do
            local entry = array[i]
            id = entry[1]
            val = entry[2]
            emit(id, val)
            start = start + 1
        end
    end

    local function merge(first, second)
        local i, j = 1, 1
        local first_len = #first
        local second_len = #second
        local emit = emit

        while i <= first_len and j <= second_len do
            local full_id_a, val_a, numeric_id_a, seq_a = unpack(first[i])
            local full_id_b, val_b, numeric_id_b, seq_b = unpack(second[j])
            local ts, val

            if (numeric_id_a < numeric_id_b) then
                ts = full_id_a
                val = val_a
                i = i + 1
            elseif (numeric_id_a > numeric_id_b) then
                ts = full_id_b
                val = val_b
                j = j + 1
            elseif (numeric_id_b == numeric_id_a) then
                seq_a = tonumber(seq_a)
                seq_b = tonumber(seq_b)
                if (seq_a < seq_b) then
                    ts = full_id_a
                    val = val_a
                    i = i + 1
                elseif (seq_a > seq_b) then
                    ts = full_id_b
                    val = val_b
                    j = j + 1
                else
                    -- doesnt matter, pick one
                    emit(full_id_a, val_a)
                    i = i + 1
                    ts = nil
                end
            end
            if ts ~= nil then
                emit(ts, val)
            end
        end
        add_remaining(first, i)
        add_remaining(second, j)

        return #first + #second
    end


    local MERGE_OPTIONS = {
        LIMIT = 1,
        FILTER = 1,
        LABELS = 1,
        REDACT = 1
    }

    local params = parse_range_params(MERGE_OPTIONS, min, max, ...)
    local first = base_range('XRANGE', firstKey, params)
    local second = base_range('XRANGE', secondKey, params)
    params.parse_ts = true

    local first_range = process_range(first, params)
    local second_range = process_range(second, params)

    return merge(first_range, second_range)
end

function Timeseries._collect(sourceKey, min, max, ...)

    local OPTIONS = {
        LIMIT = 1,
        FILTER = 1,
        LABELS = 1
    }

    local params = parse_range_params(OPTIONS, min, max, ...)
    assert(params.labels ~= nil, "At least one field must be specified")

    local range = base_range('XRANGE', sourceKey, params)
    params.parse_ts = true
    range = process_range(range, params)

    local by_key = {}
    local k
    for _, v in ipairs(range) do
        local ts = v[3]
        local hash = v[2] or {}
        for i = 1, #hash, 2 do
            k = hash[i]
            by_key[k] = by_key[k] or {}
            table.insert(by_key[k], { ts, hash[i + 1] })
        end
    end

    return by_key, params
end

--- add stats, distinct, values, count_distinct methods
function Timeseries.distinct(sourceKey, min, max, ...)
    local by_key = Timeseries._collect(sourceKey, min, max, ...)
    local result = {}
    local val

    for key, values in pairs(by_key) do
        local dedup = {}
        local list = {}
        for _, entry in ipairs(values) do
            val = entry[2]
            if dedup[val] == nil then
                dedup[val] = 1
                list[#list + 1] = val
            end
        end
        result[#result + 1] = key
        result[#result + 1] = list
    end

    return result
end

function Timeseries.count_distinct(sourceKey, min, max, ...)
    local by_key = Timeseries._collect(sourceKey, min, max, ...)
    local result = {}
    local val

    for key, values in pairs(by_key) do
        result[key] = {}
        local slot = result[key]
        for _, entry in ipairs(values) do
            val = entry[2]
            slot[val] = tonumber(slot[val] or 0) + 1
        end
    end

    return to_bulk_reply(result)
end

function Timeseries.basic_stats(sourceKey, min, max, ...)
    local by_key = Timeseries._collect(sourceKey, min, max, ...)
    local result = {}
    local val
    local possibly_convert_float = possibly_convert_float

    for key, values in pairs(by_key) do
        local data = {}
        for _, entry in ipairs(values) do
            val = tonumber(entry[2])
            if (val ~= nil) then
                data[#data + 1] = val
            end
        end

        local stat = stats.basic(data)
        local temp = {}
        for k, v in pairs(stat) do
            temp[#temp + 1] = k
            temp[#temp + 1] = possibly_convert_float(v)
        end
        result[#result + 1] = key
        result[#result + 1] = temp
    end

    return result
end

---------
local UpperMap

local command_name = assert(table.remove(ARGV, 1), 'Timeseries: must provide a command')
local command = Timeseries[command_name]
if (command == nil) then
    if UpperMap == nil then
        UpperMap = {}
        for name, func in pairs(Timeseries) do
            if name:sub(1, 1) ~= '_' then
                UpperMap[name:upper()] = func
            end
        end
    end
    command_name = string.upper(command_name)
    command = UpperMap[command_name]
end
if (command == nil) then
    error('Timeseries: unknown command ' .. command_name)
end

-- ts_debug('running ' .. command_name .. '(' .. KEYS[1] .. ',' .. table.tostring(ARGV) .. ')')

if (command_name == 'copy') or (command_name == 'COPY') then
    return command(KEYS[1], KEYS[2], unpack(ARGV))
elseif (command_name == 'merge') or (command_name == 'MERGE') then
    return command(KEYS[1], KEYS[2], KEYS[3], unpack(ARGV))
end

local result = command(KEYS[1], unpack(ARGV))

return result
