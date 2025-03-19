import type * as types from './types';
import type { ConfigOptions, FetchResponse } from 'api/dist/core'
import Oas from 'oas';
import APICore from 'api/dist/core';
import definition from './openapi.json';

class SDK {
  spec: Oas;
  core: APICore;

  constructor() {
    this.spec = Oas.init(definition);
    this.core = new APICore(this.spec, 'bluefin-exchange/v1 (api/6.1.3)');
  }

  /**
   * Optionally configure various options that the SDK allows.
   *
   * @param config Object of supported SDK options and toggles.
   * @param config.timeout Override the default `fetch` request timeout of 30 seconds. This number
   * should be represented in milliseconds.
   */
  config(config: ConfigOptions) {
    this.core.setConfig(config);
  }

  /**
   * If the API you're using requires authentication you can supply the required credentials
   * through this method and the library will magically determine how they should be used
   * within your API request.
   *
   * With the exception of OpenID and MutualTLS, it supports all forms of authentication
   * supported by the OpenAPI specification.
   *
   * @example <caption>HTTP Basic auth</caption>
   * sdk.auth('username', 'password');
   *
   * @example <caption>Bearer tokens (HTTP or OAuth 2)</caption>
   * sdk.auth('myBearerToken');
   *
   * @example <caption>API Keys</caption>
   * sdk.auth('myApiKey');
   *
   * @see {@link https://spec.openapis.org/oas/v3.0.3#fixed-fields-22}
   * @see {@link https://spec.openapis.org/oas/v3.1.0#fixed-fields-22}
   * @param values Your auth credentials for the API; can specify up to two strings or numbers.
   */
  auth(...values: string[] | number[]) {
    this.core.setAuth(...values);
    return this;
  }

  /**
   * If the API you're using offers alternate server URLs, and server variables, you can tell
   * the SDK which one to use with this method. To use it you can supply either one of the
   * server URLs that are contained within the OpenAPI definition (along with any server
   * variables), or you can pass it a fully qualified URL to use (that may or may not exist
   * within the OpenAPI definition).
   *
   * @example <caption>Server URL with server variables</caption>
   * sdk.server('https://{region}.api.example.com/{basePath}', {
   *   name: 'eu',
   *   basePath: 'v14',
   * });
   *
   * @example <caption>Fully qualified server URL</caption>
   * sdk.server('https://eu.api.example.com/v14');
   *
   * @param url Server URL
   * @param variables An object of variables to replace into the server URL.
   */
  server(url: string, variables = {}) {
    this.core.setServer(url, variables);
  }

  /**
   * Get exchange TVL, total volume, total fee and other statistics
   *
   */
  getExchangeInfo(): Promise<FetchResponse<200, types.GetExchangeInfoResponse200>> {
    return this.core.fetch('/api/v1/info', 'get');
  }

  /**
   * Get Pools Information
   *
   */
  getPoolsInfo(metadata?: types.GetPoolsInfoMetadataParam): Promise<FetchResponse<200, types.GetPoolsInfoResponse200>> {
    return this.core.fetch('/api/v1/pools/info', 'get', metadata);
  }

  /**
   * Get Pool Transactions
   *
   */
  getPoolTransactions(metadata: types.GetPoolTransactionsMetadataParam): Promise<FetchResponse<200, types.GetPoolTransactionsResponse200>> {
    return this.core.fetch('/api/v1/pool/transactions', 'get', metadata);
  }

  /**
   * Get Pool Line Data Ordered by Time Descending
   *
   */
  getPoolLine(metadata: types.GetPoolLineMetadataParam): Promise<FetchResponse<200, types.GetPoolLineResponse200>> {
    return this.core.fetch('/api/v1/pool/stats/line', 'get', metadata);
  }

  /**
   * Get Pool Ticks
   *
   */
  getPoolTicks(metadata: types.GetPoolTicksMetadataParam): Promise<FetchResponse<200, types.GetPoolTicksResponse200>> {
    return this.core.fetch('/api/v1/pool/line/ticks', 'get', metadata);
  }

  /**
   * Get Tokens Information
   *
   */
  getTokensInfo(metadata?: types.GetTokensInfoMetadataParam): Promise<FetchResponse<200, types.GetTokensInfoResponse200>> {
    return this.core.fetch('/api/v1/tokens/info', 'get', metadata);
  }

  /**
   * Get Tokens Oracle Price
   *
   */
  getTokensPrice(metadata: types.GetTokensPriceMetadataParam): Promise<FetchResponse<200, types.GetTokensPriceResponse200>> {
    return this.core.fetch('/api/v1/tokens/price', 'get', metadata);
  }
}

const createSDK = (() => { return new SDK(); })()
;

export default createSDK;

export type { GetExchangeInfoResponse200, GetPoolLineMetadataParam, GetPoolLineResponse200, GetPoolTicksMetadataParam, GetPoolTicksResponse200, GetPoolTransactionsMetadataParam, GetPoolTransactionsResponse200, GetPoolsInfoMetadataParam, GetPoolsInfoResponse200, GetTokensInfoMetadataParam, GetTokensInfoResponse200, GetTokensPriceMetadataParam, GetTokensPriceResponse200 } from './types';
