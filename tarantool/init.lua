box.cfg {
    listen = 3301
}

box.once('bootstrap_kv', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, {if_not_exists = true})

    local kv = box.schema.space.create('KV', {
        if_not_exists = true,
        format = {
            {name = 'key', type = 'string'},
            {name = 'value', type = 'varbinary', is_nullable = true}
        }
    })

    kv:create_index('primary', {
        type = 'tree',
        parts = {'key'},
        if_not_exists = true
    })

    box.schema.func.create('kv_count', {if_not_exists = true})
    box.schema.user.grant('guest', 'execute', 'function', 'kv_count', {if_not_exists = true})

    box.schema.func.create('kv_range', {if_not_exists = true})
    box.schema.user.grant('guest', 'execute', 'function', 'kv_range', {if_not_exists = true})
end)

function kv_count()
    return box.space.KV:count()
end

function kv_range(key_since, key_to, limit)
    local result = {}
    local count = 0

    for _, tuple in box.space.KV:pairs({key_since}, {iterator = 'GE'}) do
        local key = tuple[1]

        if count >= limit or key > key_to then
            break
        end

        result[#result + 1] = { tuple[1], tuple[2] }
        count = count + 1
    end

    return result
end