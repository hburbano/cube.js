import { BaseFilter, BaseQuery } from '@cubejs-backend/schema-compiler';

const GRANULARITY_TO_INTERVAL: Record<string, (data: string) => string> = {
  day: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd 00:00:00.000')`,
  week: (date) => `DATE_FORMAT(from_unixtime(unix_timestamp('1900-01-01 00:00:00') + floor((unix_timestamp(${date}) - unix_timestamp('1900-01-01 00:00:00')) / (60 * 60 * 24 * 7)) * (60 * 60 * 24 * 7)), 'yyyy-MM-dd 00:00:00.000')`,
  hour: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:00:00.000')`,
  minute: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:00.000')`,
  second: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-dd HH:mm:ss.000')`,
  month: (date) => `DATE_FORMAT(${date}, 'yyyy-MM-01 00:00:00.000')`,
  year: (date) => `DATE_FORMAT(${date}, 'yyyy-01-01 00:00:00.000')`
};

class HiveFilter extends BaseFilter {
  public likeIgnoreCase(column: any, not: any, param: any) {
    return `${column}${not ? ' NOT' : ''} LIKE CONCAT('%', ${this.allocateParam(param)}, '%')`;
  }
}

export class DatabricksQuery extends BaseQuery {
  public newFilter(filter: any) {
    return new HiveFilter(this, filter);
  }

  public convertTz(field: string) {
    return `from_utc_timestamp(${field}, '${this.timezone}')`;
  }

  public timeStampCast(value: string) {
    return `from_utc_timestamp(replace(replace(${value}, 'T', ' '), 'Z', ''), 'UTC')`;
  }

  public dateTimeCast(value: string) {
    return `from_utc_timestamp(${value}, 'UTC')`; // TODO
  }

  // subtractInterval(date, interval) {
  //   return `DATE_SUB(${date}, INTERVAL ${interval})`; // TODO
  // }

  // addInterval(date, interval) {
  //   return `DATE_ADD(${date}, INTERVAL ${interval})`; // TODO
  // }

  public timeGroupedColumn(granularity: string, dimension: string) {
    return GRANULARITY_TO_INTERVAL[granularity](dimension);
  }

  public escapeColumnName(name: string) {
    return `\`${name}\``;
  }

  public getFieldIndex(id: string) {
    const dimension = this.dimensionsForSelect().find((d: any) => d.dimension === id);
    if (dimension) {
      return super.getFieldIndex(id);
    }
    return this.escapeColumnName(this.aliasName(id, false));
  }

  public unixTimestampSql() {
    return 'unix_timestamp()';
  }

  public defaultRefreshKeyRenewalThreshold() {
    return 120;
  }
}
