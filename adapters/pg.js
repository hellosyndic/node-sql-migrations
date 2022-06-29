var Client = require('pg-native')
var client = new Client()
client.connectSync()

module.exports = function (config, logger) {

    function exec(query, values) {
       console.log(query);
	    return new Promise((resolve, reject) => {
    		resolve(client.querySync(query,values));
	    });
    }

    function ensureMigrationTableExists() {
        return exec('create table if not exists "__migrations__" (id bigint NOT NULL)');
    }

    return {
        appliedMigrations: function appliedMigrations() {
          return ensureMigrationTableExists().then(function () {
                return exec('select * from __migrations__');
            }).then(function (result) {

                return result.map(function (row) { return row.id; });
            }).catch(function(e){
	    	console.log(e);
	    });
        },
        applyMigration: function applyMigration(migration, sql) {
            return exec(sql).then(function (result) {
                logger.log('Applying ' + migration);
                logger.log(result)
                logger.log('===============================================');
                var values = [migration.match(/^(\d)+/)[0]];
                return exec('insert into __migrations__ (id) values ($1)', values);
            });
        },
        rollbackMigration: function rollbackMigration(migration, sql) {
            return exec(sql).then(function (result) {
                logger.log('Reverting ' + migration);
                logger.log(result)
                logger.log('===============================================');
                var values = [migration.match(/^(\d)+/)[0]];
                return exec('delete from __migrations__ where id = $1', values);
            });
        },
        dispose: function dispose() {
            return client.end();
        }
    };
};