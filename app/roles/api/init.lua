local cartridge = require('cartridge')
local checks = require('checks')
local errors = require('errors')
local http_client = require('http.client')
local json = require('json')
local log = require('log')

local err_vshard_router = errors.new_class('Vshard routing error')
local err_http_client = errors.new_class('HTTP client error')
local err_httpd = errors.new_class('HTTPD error')
local err_request_params = errors.new_class('Request params')

-- forecast service url with default value
local weather_forecast_service_url
    = 'https://api.open-meteo.com/v1/forecast?latitude=%s&longitude=%s&hourly=temperature_2m&forecast_days=1'

local exports = {}

-- http request for weather forecast
-- ex:
-- https://api.open-meteo.com/v1/forecast?latitude=55.75&longitude=37.62&hourly=temperature_2m&forecast_days=1
local function make_weather_query(longitude, latitude)
    local address = string.format(weather_forecast_service_url, latitude, longitude)
    local response = http_client.get(address)

    if response.status ~= 200 then
        return nil, err_http_client:new('unexpected weather service response')
    end

    return response.body
end

-- http bad response helper
local function bad_response(req, desc, status)
    local resp = req:render({json = {error = desc}})
    resp.status = status

    return resp
end

-- http good response helper
local function ok_response(req, body)
    local resp = req:render({json = body})
    resp.status = 200

    return resp
end

local function calc_bucket_by_coords(longitude)
    checks('number')

    -- normalize longitude to positive
    local longitude = longitude + 180

    return math.ceil(longitude)
end

-- make forecast object helper
local function build_forecast_object(raw_forecast, bucket_id, longitude, latitude)
    checks('string', 'number', 'number', 'number')

    local forecast = json.decode(raw_forecast)
    forecast.raw_data = raw_forecast
    forecast.bucket_id = bucket_id
    -- HACK: weather service sometimes respond slightly another coords
    -- we use requested coords for save into cache (tarantool)
    forecast.latitude = latitude
    forecast.longitude = longitude

    return forecast
end

local function validate_request_params(req)
    local latitude = req:query_param('latitude')
    local longitude = req:query_param('longitude')

    -- check coords present
    longitude = tonumber(longitude)
    latitude = tonumber(latitude)

    if not longitude or not latitude then
        return nil, err_request_params:new('longitude or latitude not provided or in wrong format')
    end

    -- check coords in bounds
    if longitude < -180 or longitude > 180 then
        return nil, err_request_params:new('longitude have wrong value: ' .. longitude)
    end

    if latitude < -90 or latitude > 90 then
        return nil, err_request_params:new('latitude have wrong value: ' .. latitude)
    end

    return true
end

-- делаем запрос в тарантул и пытаемся получить данные
-- если данные есть:
--      отдаем их клиенту
-- если данных нет:
--      делаем запрос к сервису прогноза погоды
--      сохраняем в тарантул
--      отдаем клиенту
local function http_weather_get_handler(req)
    local valid, err = validate_request_params(req)
    if not valid then
        log.info(err)

        return bad_response(req, err.err, 422)
    end

    local latitude = req:query_param('latitude')
    local longitude = req:query_param('longitude')

    longitude = tonumber(longitude)
    latitude = tonumber(latitude)

    -- get router and create bucket id
    local router = cartridge.service_get('vshard-router').get()
    local bucket_by_data = calc_bucket_by_coords(longitude, latitude)
    local bucket_id = router:bucket_id(bucket_by_data)

    -- trying to get weather forecast from cache (taranrool)
    local t_resp, err = err_vshard_router:pcall(
        router.call,
        router,
        bucket_id,
        'read',
        'weather_forecast_get',
        {longitude, latitude}
    )
    -- tarantool request error
    if err then
        log.error(err)

        return bad_response(req, err.err, 500)
    end

    -- successfully retrieved data from cache (taranrool)
    if t_resp then
        return ok_response(req, json.decode(t_resp.raw_data))
    end

    -- make request to weather service
    local raw_forecast, err = err_http_client:pcall(
        make_weather_query,
        longitude,
        latitude
    )
    if err then
        log.error(err)

        return bad_response(req, err.err, 500)
    end

    local forecast = build_forecast_object(raw_forecast, bucket_id, longitude, latitude)

    -- trying to save into cache (tarantool)
    local _, err = err_vshard_router:pcall(
        router.call,
        router,
        bucket_id,
        'write',
        'weather_forecast_add',
        {forecast}
    )
    if err then log.error(err) end

    return ok_response(req, json.decode(forecast.raw_data))
end

-- validate config
-- some basic validation
exports.validate_config = function(cfg)
    log.verbose('validate config')

    -- HACK on startup - defaul value of provided config is nil
    -- it's ok - we will handle it on apply_config
    if cfg.weather_forecast_service_url ~= nil then
        assert(type(cfg.weather_forecast_service_url) == 'string', 'weather_forecast_service_url must be a string')
    end

    return true
end

-- apply config
exports.apply_config = function(conf)
    log.verbose('apply config')

    -- HACK if we get nil value - previous value saved
    weather_forecast_service_url = conf.weather_forecast_service_url or weather_forecast_service_url
end

exports.init = function()
    log.verbose('init api')

    local httpd = cartridge.service_get('httpd')

    if not httpd then
        return nil, err_httpd:new("httpd service create")
    end

    -- http handlers
    httpd:route(
        { path = '/weather', method = 'GET', public = true },
        http_weather_get_handler
    )

    return true
end

return exports
