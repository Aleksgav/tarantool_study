local cache = require('app/roles/throughput-cache/init')

return {
    role_name = 'throughput-cache',
    init = cache.init,
    dependencies = {
        'cartridge.roles.vshard-storage',
    },
}
