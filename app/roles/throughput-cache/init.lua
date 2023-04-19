local checks = require('checks')
local log = require('log')

local exports = {}

local function weather_forecast_add(forecast)
    checks('table')

    log.verbose('weather forecast add')

    box.space.weather_forecast:insert({
        forecast.longitude,
        forecast.latitude,
        forecast.bucket_id,
        forecast.raw_data,
    })

    return true
end

local function weather_forecast_get(longitude, latitude)
    checks('number', 'number')

    log.verbose('weather forecast get')

    local raw_forecast = box.space.weather_forecast:get({ longitude, latitude })
    if not raw_forecast then
        return false
    end

    return raw_forecast:tomap({ names_only = true })
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

    return true
end

exports.init = function(opts)
    log.verbose('init cache')

    if opts.is_master then
        init_spaces()

        box.schema.func.create('weather_forecast_add', {if_not_exists = true})
        box.schema.func.create('weather_forecast_get', {if_not_exists = true})
    end

    rawset(_G, 'weather_forecast_add', weather_forecast_add)
    rawset(_G, 'weather_forecast_get', weather_forecast_get)

    return true
end

return exports
