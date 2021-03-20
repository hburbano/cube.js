import { BaseQuery } from '@cubejs-backend/schema-compiler';
export declare class DruidQuery extends BaseQuery {
    timeGroupedColumn(granularity: string, dimension: string): string;
    convertTz(field: string): string;
    subtractInterval(date: string, interval: string): string;
    addInterval(date: string, interval: string): string;
    timeStampCast(value: string): string;
    timeStampParam(): string;
    nowTimestampSql(): string;
}
//# sourceMappingURL=DruidQuery.d.ts.map