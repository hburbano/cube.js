export declare type DruidClientBaseConfiguration = {
    user?: string;
    password?: string;
    database?: string;
};
export declare type DruidClientConfiguration = DruidClientBaseConfiguration & {
    url: string;
};
export declare class DruidClient {
    protected readonly config: DruidClientConfiguration;
    constructor(config: DruidClientConfiguration);
    protected getClient(): import("axios").AxiosInstance;
    cancel(queryId: string): Promise<import("axios").AxiosResponse<any>>;
    query(query: string, parameters: {
        type: string;
        value: unknown;
    }[]): Promise<unknown[]>;
}
//# sourceMappingURL=DruidClient.d.ts.map