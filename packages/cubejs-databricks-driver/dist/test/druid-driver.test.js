"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// eslint-disable-next-line import/no-extraneous-dependencies
const testcontainers_1 = require("testcontainers");
// eslint-disable-next-line import/no-extraneous-dependencies
const path_1 = __importDefault(require("path"));
const DruidDriver_1 = require("../src/DruidDriver");
describe('DruidDriver', () => {
    let env = null;
    let config;
    const doWithDriver = async (callback) => {
        const driver = new DruidDriver_1.DruidDriver(config);
        await callback(driver);
    };
    // eslint-disable-next-line consistent-return
    beforeAll(async () => {
        if (process.env.TEST_DRUID_HOST) {
            const host = process.env.TEST_DRUID_HOST || 'localhost';
            const port = process.env.TEST_DRUID_PORT || '8888';
            config = {
                url: `http://${host}:${port}`,
            };
            return;
        }
        const dc = new testcontainers_1.DockerComposeEnvironment(path_1.default.resolve(path_1.default.dirname(__filename), '../../'), 'docker-compose.yml');
        env = await dc
            .withWaitStrategy('zookeeper', testcontainers_1.Wait.forLogMessage('binding to port /0.0.0.0:2181'))
            .withWaitStrategy('postgres', testcontainers_1.Wait.forHealthCheck())
            .withWaitStrategy('router', testcontainers_1.Wait.forHealthCheck())
            .withWaitStrategy('middlemanager', testcontainers_1.Wait.forHealthCheck())
            .withWaitStrategy('historical', testcontainers_1.Wait.forHealthCheck())
            .withWaitStrategy('broker', testcontainers_1.Wait.forHealthCheck())
            .withWaitStrategy('coordinator', testcontainers_1.Wait.forHealthCheck())
            .up();
        const host = env.getContainer('router').getHost();
        const port = env.getContainer('router').getMappedPort(8888);
        config = {
            user: 'admin',
            password: 'password1',
            url: `http://${host}:${port}`,
        };
    }, 2 * 60 * 1000);
    // eslint-disable-next-line consistent-return
    afterAll(async () => {
        if (env) {
            await env.down();
        }
    }, 30 * 1000);
    it('should construct', async () => {
        jest.setTimeout(10 * 1000);
        return doWithDriver(async () => {
            //
        });
    });
    it('should test connection', async () => {
        jest.setTimeout(10 * 1000);
        return doWithDriver(async (driver) => {
            await driver.testConnection();
        });
    });
    it('SELECT 1', async () => {
        jest.setTimeout(10 * 1000);
        return doWithDriver(async (driver) => {
            expect(await driver.query('SELECT 1')).toEqual([{
                    EXPR$0: 1,
                }]);
        });
    });
});
//# sourceMappingURL=druid-driver.test.js.map