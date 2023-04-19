local api = require('app/roles/api/init')

return {
    role_name = 'api',
    init = api.init,
    apply_config = api.apply_config,
    validate_config = api.validate_config,
    dependencies = {'cartridge.roles.vshard-router'},
}
