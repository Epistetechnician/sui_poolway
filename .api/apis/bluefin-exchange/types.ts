import type { FromSchema } from 'json-schema-to-ts';
import * as schemas from './schemas';

export type GetExchangeInfoResponse200 = FromSchema<typeof schemas.GetExchangeInfo.response['200']>;
export type GetPoolLineMetadataParam = FromSchema<typeof schemas.GetPoolLine.metadata>;
export type GetPoolLineResponse200 = FromSchema<typeof schemas.GetPoolLine.response['200']>;
export type GetPoolTicksMetadataParam = FromSchema<typeof schemas.GetPoolTicks.metadata>;
export type GetPoolTicksResponse200 = FromSchema<typeof schemas.GetPoolTicks.response['200']>;
export type GetPoolTransactionsMetadataParam = FromSchema<typeof schemas.GetPoolTransactions.metadata>;
export type GetPoolTransactionsResponse200 = FromSchema<typeof schemas.GetPoolTransactions.response['200']>;
export type GetPoolsInfoMetadataParam = FromSchema<typeof schemas.GetPoolsInfo.metadata>;
export type GetPoolsInfoResponse200 = FromSchema<typeof schemas.GetPoolsInfo.response['200']>;
export type GetTokensInfoMetadataParam = FromSchema<typeof schemas.GetTokensInfo.metadata>;
export type GetTokensInfoResponse200 = FromSchema<typeof schemas.GetTokensInfo.response['200']>;
export type GetTokensPriceMetadataParam = FromSchema<typeof schemas.GetTokensPrice.metadata>;
export type GetTokensPriceResponse200 = FromSchema<typeof schemas.GetTokensPrice.response['200']>;
