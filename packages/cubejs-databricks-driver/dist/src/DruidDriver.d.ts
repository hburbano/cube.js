import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { DruidClient, DruidClientBaseConfiguration } from './DruidClient';
import { DruidQuery } from './DruidQuery';
export declare type DruidDriverConfiguration = DruidClientBaseConfiguration & {
    url: string;
};
export declare class DruidDriver extends BaseDriver {
    protected readonly config: DruidDriverConfiguration;
    protected readonly client: DruidClient;
    static dialectClass(): typeof DruidQuery;
    constructor(config?: DruidDriverConfiguration);
    readOnly(): boolean;
    testConnection(): Promise<void>;
    query(query: string, values?: unknown[]): Promise<Array<unknown>>;
    informationSchemaQuery(): string;
    createSchemaIfNotExists(schemaName: string): Promise<unknown[]>;
    getTablesQuery(schemaName: string): Promise<unknown[]>;
    protected normalizeQueryValues(values: unknown[]): {
        value: unknown;
        type: string;
    }[];
    protected normaliseResponse(res: any): any;
}
//# sourceMappingURL=DruidDriver.d.ts.map