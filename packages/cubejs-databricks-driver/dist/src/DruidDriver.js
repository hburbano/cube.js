"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DruidDriver = void 0;
const query_orchestrator_1 = require("@cubejs-backend/query-orchestrator");
const shared_1 = require("@cubejs-backend/shared");
const DruidClient_1 = require("./DruidClient");
const DruidQuery_1 = require("./DruidQuery");
class DruidDriver extends query_orchestrator_1.BaseDriver {
    constructor(config) {
        super();
        let url = (config === null || config === void 0 ? void 0 : config.url) || process.env.CUBEJS_DB_URL;
        if (!url) {
            const host = process.env.CUBEJS_DB_HOST;
            const port = process.env.CUBEJS_DB_PORT;
            if (host && port) {
                const protocol = shared_1.getEnv('dbSsl') ? 'https' : 'http';
                url = `${protocol}://${host}:${port}`;
            }
            else {
                throw new Error('Please specify CUBEJS_DB_URL');
            }
        }
        this.config = {
            url,
            user: (config === null || config === void 0 ? void 0 : config.user) || process.env.CUBEJS_DB_USER,
            password: (config === null || config === void 0 ? void 0 : config.password) || process.env.CUBEJS_DB_PASS,
            database: (config === null || config === void 0 ? void 0 : config.database) || process.env.CUBEJS_DB_NAME || (config === null || config === void 0 ? void 0 : config.database) || 'default',
            ...config,
        };
        this.client = new DruidClient_1.DruidClient(this.config);
    }
    static dialectClass() {
        return DruidQuery_1.DruidQuery;
    }
    readOnly() {
        return true;
    }
    async testConnection() {
        //
    }
    async query(query, values = []) {
        return this.client.query(query, this.normalizeQueryValues(values));
    }
    informationSchemaQuery() {
        return `
        SELECT
            COLUMN_NAME as ${this.quoteIdentifier('column_name')},
            TABLE_NAME as ${this.quoteIdentifier('table_name')},
            TABLE_SCHEMA as ${this.quoteIdentifier('table_schema')},
            DATA_TYPE as ${this.quoteIdentifier('data_type')}
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')
    `;
    }
    async createSchemaIfNotExists(schemaName) {
        throw new Error('Unable to create schema, Druid does not support it');
    }
    async getTablesQuery(schemaName) {
        return this.query('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?', [
            schemaName
        ]);
    }
    normalizeQueryValues(values) {
        return values.map((value) => ({
            value,
            type: 'VARCHAR',
        }));
    }
    normaliseResponse(res) {
        return res;
    }
}
exports.DruidDriver = DruidDriver;
//# sourceMappingURL=DruidDriver.js.map