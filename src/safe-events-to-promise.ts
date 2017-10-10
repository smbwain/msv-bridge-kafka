
import EventEmitter = NodeJS.EventEmitter;

export default function (emitter: EventEmitter, successEvents : string | string[], errorEvents : string | string[] = 'error') : Promise<any> {

    successEvents = Array.isArray(successEvents) ? successEvents : [successEvents];
    errorEvents = Array.isArray(errorEvents) ? errorEvents : [errorEvents];

    let alreadyRejected, alreadyResolved;

    let deffered;
    let promise = new Promise((resolve, reject) => {
        deffered = {
            resolve, reject
        };
    });

    function cleanup() {
        for(const errorEvent of errorEvents) {
            emitter.removeListener(errorEvent, errorHandler);
        }
        for(const successEvent of successEvents) {
            emitter.removeListener(successEvent, successHandler);
        }
    }
    function errorHandler(err) {
        cleanup();
        deffered && deffered.reject(err);
        alreadyRejected = err;
    }
    function successHandler() {
        cleanup();
        deffered && deffered.resolve(arguments);
        alreadyResolved = arguments;
    }

    for(const errorEvent of errorEvents) {
        emitter.on(errorEvent, errorHandler);
    }
    if(alreadyRejected) {
        return Promise.reject(alreadyRejected);
    }

    for(const successEvent of successEvents) {
        emitter.on(successEvent, successHandler);
    }
    if(alreadyResolved) {
        return Promise.resolve(alreadyResolved);
    }

    return promise;
}