local log = require('log')

local function weather_forecast_add(forecast)
    log.info('weather forecast add')

    box.space.weather_forecast:insert({
        forecast.longitude,
        forecast.latitude,
        forecast.bucket_id,
        forecast.raw_data,
    })
end

local function weather_forecast_get(longitude, latitude)
    log.info('weather forecast get')

    local raw_forecast = box.space.weather_forecast:get({ longitude, latitude })
    if not raw_forecast then
        return false
    end

    return forecast_tuple_to_map(raw_forecast)
end

function forecast_tuple_to_map(raw_forecast)
    return {
        longitude = raw_forecast[1],
        latitude = raw_forecast[2],
        bucket_id = raw_forecast[3],
        raw_data = raw_forecast[4]
    }
end

local function init_spaces()
    local weather_forecast= box.schema.space.create(
        'weather_forecast',
        {
            format = {
                {'longitude', 'number'},
                {'latitude', 'number'},
                {'bucket_id', 'unsigned'},
                {'raw_data', 'string'}
            },
            if_not_exists = true,
            engine = 'memtx',
        }
    )

    weather_forecast:create_index('place', {
        parts = {'longitude', 'latitude'},
        unique = true,
        if_not_exists = true,
    })

    weather_forecast:create_index('longitude', {
        parts = {'longitude'},
        unique = false,
        if_not_exists = true,
    })

    weather_forecast:create_index('latitude', {
        parts = {'latitude'},
        unique = false,
        if_not_exists = true,
    })

    weather_forecast:create_index('bucket_id', {
        parts = {'bucket_id'},
        unique = false,
        if_not_exists = true,
    })
end

local function init(opts)
    log.info('init cache')

    if opts.is_master then
        init_spaces()

        box.schema.func.create('weather_forecast_add', {if_not_exists = true})
        box.schema.func.create('weather_forecast_get', {if_not_exists = true})
    end

    rawset(_G, 'weather_forecast_add', weather_forecast_add)
    rawset(_G, 'weather_forecast_get', weather_forecast_get)

    return true
end

return {
    role_name = 'throughput-cache',
    init = init,
    dependencies = {
        'cartridge.roles.vshard-storage',
    },
    utils = {
        weather_forecast_add = weather_forecast_add,
        weather_forecast_get = weather_forecast_get,
    }
}
