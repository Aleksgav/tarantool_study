local cartridge = require('cartridge')
local errors = require('errors')
local http_client = require('http.client')
local json = require('json')
local log = require('log')

local err_vshard_router = errors.new_class('Vshard routing error')
local err_http_client = errors.new_class('HTTP client error')

-- делаем запрос в тарантул и пытаемся получить данные
-- если данные есть:
--      отдаем их клиенту
-- если данных нет:
--      делаем запрос к сервису прогноза погоды
--      сохраняем в тарантул
--      отдаем клиенту
local function http_weather_get_handler(req)
    local query = req.query

    local latitude = req:query_param('latitude')
    local longitude = req:query_param('longitude')

    -- if long or lat empty
    if not longitude or not latitude then
        -- return client error
        return bad_response(req, 'longitude or latitude error', 400)
    end

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
        log.error(err.message)

        return bad_response(req, err.message, 500)
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
        log.error(err.message)

        return bad_response(req, err.message, 500)
    end

    local forecast = json.decode(raw_forecast)
    forecast.raw_data = raw_forecast
    forecast.bucket_id = bucket_id
    -- HACK: weather service sometimes respond slightly another coords
    -- we use requested coords for save into cache (tarantool)
    forecast.latitude = latitude
    forecast.longitude = longitude

    -- trying to save into cache (tarantool)
    local t_resp, err = err_vshard_router:pcall(
        router.call,
        router,
        bucket_id,
        'write',
        'weather_forecast_add',
        {forecast}
    )
    if err then
        log.error(err.message)

        return bad_response(req, err.message, 500)
    end

    return ok_response(req, json.decode(forecast.raw_data))
end

-- bucket, value: -180 0 180
function calc_bucket_by_coords(longitude, latitude)
    return math.ceil(longitude)
end

-- http bad response helper
function bad_response(req, desc, status)
    local resp = req:render({json = {error = desc}})
    resp.status = status

    return resp
end

-- http good response helper
function ok_response(req, body)
    local resp = req:render({json = body})
    resp.status = 200

    return resp
end

-- http request for weather forecast
-- ex:
-- https://api.open-meteo.com/v1/forecast?latitude=55.75&longitude=37.62&hourly=temperature_2m&forecast_days=1
function make_weather_query(longitude, latitude)
    local address = string.format('https://api.open-meteo.com/v1/forecast?latitude=%s&longitude=%s&hourly=temperature_2m&forecast_days=1', latitude, longitude)
    local response = http_client.get(address)

    if response.status ~= 200 then error({ desc = "unexpected weather service response" }) end

    return response.body
end

local function init(opts)
    if opts.is_master then
        box.schema.user.grant('guest',
            'read,write,execute',
            'universe',
            nil, { if_not_exists = true }
        )
    end

    log.error('init api')

    local httpd = cartridge.service_get('httpd')

    if not httpd then
        return nil, err_httpd:new("not found")
    end

    -- http handlers
    httpd:route(
        { path = '/weather', method = 'GET', public = true },
        http_weather_get_handler
    )

    return true
end

return {
    role_name = 'api',
    init = init,
    dependencies = {'cartridge.roles.vshard-router'},
}
