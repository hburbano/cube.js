"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DruidClient = void 0;
const axios_1 = __importDefault(require("axios"));
class DruidClient {
    constructor(config) {
        this.config = config;
    }
    getClient() {
        const config = {
            baseURL: this.config.url,
            headers: {
                'Content-Type': 'application/json',
            }
        };
        if (this.config.user && this.config.password) {
            config.auth = {
                username: this.config.user,
                password: this.config.password,
            };
        }
        return axios_1.default.create(config);
    }
    async cancel(queryId) {
        return this.getClient().request({
            url: `/druid/v2/${queryId}`,
            method: 'DELETE',
        });
    }
    async query(query, parameters) {
        let cancelled = false;
        const cancelObj = {};
        const promise = (async () => {
            cancelObj.cancel = async () => {
                cancelled = true;
            };
            try {
                const response = await this.getClient().request({
                    url: '/druid/v2/sql/',
                    method: 'POST',
                    data: {
                        query,
                        parameters,
                        resultFormat: 'object',
                    },
                });
                if (cancelled) {
                    await this.cancel(response.headers['x-druid-sql-query-id']);
                    throw new Error('Query cancelled');
                }
                return response.data;
            }
            catch (e) {
                if (cancelled) {
                    throw new Error('Query cancelled');
                }
                if (e.response && e.response.data) {
                    if (e.response.data.errorMessage) {
                        throw new Error(e.response.data.errorMessage);
                    }
                }
                throw e;
            }
        })();
        promise.cancel = () => cancelObj.cancel();
        return promise;
    }
}
exports.DruidClient = DruidClient;
//# sourceMappingURL=DruidClient.js.map