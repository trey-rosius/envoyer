import { util } from "@aws-appsync/utils";
import { query } from "@aws-appsync/utils/dynamodb";
export const request = (ctx) => {
  const { limit = 10, nextToken } = ctx.args;
  return {
    version: "2018-05-29",
    method: "GET",
    params: {
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Request-Headers": "*",
        Accept: "application/json",
      },
    },
    resourcePath: `/get_all_emails`,
  };
};

export const response = (ctx) => {
  console.log(`response is ${ctx.result}`);
  const res = JSON.parse(ctx.result.body);
  return {
    items: res,
    nextToken: ctx.result.nextToken,
  };
};
