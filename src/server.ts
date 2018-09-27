import Koa from "koa";
import interval, { IIntervalPromiseOptions } from "interval-promise"
import { loadSettings, Settings, APP_NAME, APP_VERSION, ENV_INFO, startAppInsights } from "./common";
import { LogService, LogLevel } from "./services/logService";
import { RippleService } from "./services/rippleService";

const jsonMime = "application/json; charset=utf-8";

startAppInsights();

loadSettings()
    .then(settings => {

        const log = new LogService(settings);
        const ripple = new RippleService(settings, log);
        const koa = new Koa();

        // error handling middleware

        koa.use(async (ctx, next) => {
            try {
                await next();
            } catch (err) {

                // In case of single endpoint (GET /api/isalive) we don't need extended error handling.
                // In other cases we might want to log request/response body(ies) as context.

                // log error

                await log.write(err.status && err.status < 500 ? LogLevel.warning : LogLevel.error,
                    "api", ctx.url, err.message, undefined, err.name, err.stack);

                // return error info to client

                ctx.status = err.status || 500;
                ctx.type = jsonMime;
                ctx.body = JSON.stringify({ errorMessage: err.message });
            }
        });

        // GET /api/isalive

        koa.use(async (ctx, next) => {
            if (ctx.URL.pathname.toLowerCase() !== "/api/isalive") {
                ctx.throw(404);
            } else {
                ctx.type = jsonMime;
                ctx.body = JSON.stringify({
                    name: APP_NAME,
                    version: APP_VERSION,
                    env: ENV_INFO
                });
            }
        });

        // start http server

        koa.listen(5000);

        // start job

        const opts = { stopOnError: false };
        const func = async () => {
            try {
                const lastClosedLedger = await ripple.handleActions();
                await ripple.handleExpired(lastClosedLedger);
            } catch (err) {
                await log.write(LogLevel.error, RippleService.name, ripple.handleActions.name, err.message, undefined, err.name, err.stack);
            }
        };

        interval(func, settings.RippleJob.Interval, opts);
    })
    .then(
        _ => console.log("Started"),
        e => console.log(e));