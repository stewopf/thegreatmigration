
import {createLogger, addColors, format, transports} from 'winston';
const {combine, printf, timestamp,colorize, simple, errors, prettyPrint, splat} = format;
import { asyncLocalStorage, getKey} from './model/asyncStorage.mjs';
/**
  { 
  emerg: 0, 
  alert: 1, 
  crit: 2, 
  error: 3, 
  warning: 4, 
  notice: 5, 
  info: 6, 
  debug: 7
}
 */

const logLevels = {
    levels: {
      error: 0,
      warn: 1,
      info: 2,
      http: 3,
      sql: 4,
      debug: 5
    },
    colors: {
      error: "brightRed",
      warn: "brightMagenta",
      info: "green",
      http: "green",
      sql: "blue",
      debug: "gray"
    }
};
addColors(logLevels);

import * as path  from 'path';  
const moduleId = format((info, opts) => {
    if (opts.module) {
        info.moduleId =  path.basename(opts.module);
    }
    return info;
});

const uuidFormat = format((info, opts) => {
  return {
    ...info,
    context: asyncLocalStorage.getStore()
  }
});
function wrapLogger(module) {
    const logger = createLogger({
        format: combine(
            errors({ stack: true }), // <-- use errors format
            colorize(),        
            timestamp({format: 'YYYY-MM-DD HH:mm:ss'}),
            moduleId({module}),
            uuidFormat(),
            splat(),
            printf(info => {
                const traceId = info.context && info.context.traceId;              
                return `${info.timestamp} ${(traceId && ('[' + traceId + ']')) || '' }[${info.level}][${info.moduleId}]: ${info.message}`;
            })
        ),    
        transports: [
          new transports.Console({colorize: true, level: 'debug', label: "hello"}),
        ]
      });
    return logger;
}
export default wrapLogger;
