var _defaults = require("lodash.defaults");
var _forEach = require("lodash.foreach");
var _groupBy = require("lodash.groupby");
var _has = require("lodash.has");
var _head = require("lodash.head");
var _invert = require("lodash.invert");
var _isEmpty = require("lodash.isempty");
var _join = require("lodash.join");
var _keys = require("lodash.keys");
var _map = require("lodash.map");
var _merge = require("lodash.merge");
var _omit = require("lodash.omit");
var async = require("async");
var engines = require([__dirname, "..", "engines"].join("/"));
var fs = require("fs");
var mkdirp = require("mkdirp");

function logToContainer(core, containerId, mesg) {
    const basePath = process.env.CSHIP_LOG_PATH || "/var/log/containership"
    const directory = _join([basePath, "applications", "containership-logs", containerId], "/");

    mkdirp(directory, (err) => {
        if(err) {
            core.loggers["containership.scheduler"].log("error", "Error creating application log directory: " + err);
        } else {
            const path = _join([directory, "stdout"], "/");

            fs.appendFile(path, mesg + "\n", (err) => {
                if(err) {
                    core.loggers["containership.scheduler"].log("error", "Error logging to container: " + err);
                }
            });
        }
    });
}

module.exports = function(core){

    // initialize available engines
    _forEach(engines, function(engine, engine_name){
        engine.initialize(core);
    });

    // register codexd middleware
    _forEach(engines, function(engine, engine_name){
        engine.add_pre_start_middleware("CS_PROC_OPTS", function(options, fn){
            var application_name = options.application_name;
            var container = _omit(options, ["application_name", "start_args"]);

            var env_vars = container.env_vars;
            env_vars.CS_PROC_OPTS = JSON.stringify(core.options);
            container.env_vars = env_vars;

            var myriad_key = [core.constants.myriad.CONTAINERS_PREFIX, application_name, container.id].join(core.constants.myriad.DELIMITER);
            core.cluster.myriad.persistence.set(myriad_key, JSON.stringify(container), fn);
        });

        engine.add_pre_start_middleware("codexd", function(options, fn){
            var application_name = options.application_name;
            var container = _omit(options, ["application_name", "start_args"]);

            var volumes = _groupBy(container.volumes, function(volume){
                return volume.host;
            });

            var codexd_volumes_metadata = {};

            async.each(volumes.undefined || [], function(volume, fn){
                var uuid = core.cluster.codexd.create_uuid();

                core.cluster.codexd.create_volume({
                    id: uuid
                }, function(err){
                    if(err)
                        return fn();

                    codexd_volumes_metadata[uuid] = volume.container;
                    return fn();
                });
            }, function(err){
                var codexd_volumes_inverse = _invert(codexd_volumes_metadata);
                container.volumes = _map(container.volumes, function(volume){
                    if(volume.host == undefined && _has(codexd_volumes_inverse, volume.container)){
                        return {
                            host: [core.cluster.codexd.options.base_path, codexd_volumes_inverse[volume.container]].join("/"),
                            container: volume.container
                        }
                    }
                    else
                        return volume;
                });

                if(!_has(container.tags, "metadata"))
                    container.tags.metadata = {};

                if(!_has(container.tags.metadata, "codexd"))
                    container.tags.metadata.codexd = {};

                if(!_has(container.tags.metadata.codexd, "volumes")){
                    container.tags.metadata.codexd.volumes = codexd_volumes_metadata;
                    _merge(options, container);
                    core.cluster.myriad.persistence.set([core.constants.myriad.CONTAINERS_PREFIX, application_name, container.id].join(core.constants.myriad.DELIMITER), JSON.stringify(container), function(err){
                        return fn();
                    });
                }
                else{
                    var codexd_errs = [];

                    var host_volumes = core.cluster.codexd.get_volumes();

                    async.each(_keys(container.tags.metadata.codexd.volumes), function(uuid, fn){
                        if(!_has(host_volumes, uuid)){
                            core.cluster.codexd.get_snapshot(uuid, function(err){
                                if(err)
                                    codexd_errs.push(err);
                                else{
                                    core.cluster.legiond.send({
                                        event: core.cluster.codexd.constants.REMOVE_SNAPSHOT,
                                        data: {
                                          id: uuid
                                        }
                                    });
                                }
                                return fn();
                            });
                        }
                        else
                            return fn();
                    }, function(){
                        container.tags.metadata.codexd.volumes = _defaults(container.tags.metadata.codexd.volumes, codexd_volumes_metadata);
                        _merge(options, container);
                        core.cluster.myriad.persistence.set([core.constants.myriad.CONTAINERS_PREFIX, application_name, container.id].join(core.constants.myriad.DELIMITER), JSON.stringify(container), function(err){
                            if(_isEmpty(codexd_errs))
                                return fn();
                            else
                                return fn(_head(codexd_errs));
                        });
                    });
                }
            });
        });
    });

    return {

        container: {
            start: function(configuration){
                var container = configuration.container;
                container.application_name = configuration.application;

                if(_has(container, "engine") && _has(engines, container.engine)) {
                    engines[container.engine].start(container);
                    logToContainer(core, container.id, _join(["Starting application", container.application_name, "on container", container.id], " "));
                } else {
                    var error = new Error("Unsupported engine provided");
                    error.details = ["Engine", container.engine, "not found!"].join(" ");
                    core.loggers["containership.scheduler"].log("error", error.details);
                    containers.status = "unloaded";
                    core.cluster.myriad.persistence.set([core.constants.myriad.CONTAINERS_PREFIX, container.application_name, container.id].join("::"), function(err){
                        if(err){
                            var error = new Error("Unable to set container status");
                            error.details = ["Could not set status for", container.application_name, "container", container.id, "to 'unloaded'"].join(" ");
                            core.loggers["containership.scheduler"].log("error", error.details);
                        }
                    });
                }
            },

            stop: function(configuration){
                engines[configuration.engine].stop(configuration);
                logToContainer(core, configuration.container_id, _join(["Stopping application:", configuration.application, "on container", configuration.container_id], " "));
            },

            reconcile: function(leader){
                _forEach(engines, function(engine, engine_name){
                    engines[engine_name].reconcile(leader);
                });
            },

            add_pre_start_middleware: function(engine_name, middleware_name, fn){
                if(_has(engines, engine_name))
                    engines[engine_name].add_pre_start_middleware(middleware_name, fn);
            },

            add_pre_pull_middleware: function(engine_name, middleware_name, fn){
                if(_has(engines, engine_name))
                    engines[engine_name].add_pre_pull_middleware(middleware_name, fn);
            },

            set_start_arguments: function(engine_name, key, value){
                if(_has(engines, engine_name))
                    engines[engine_name].set_start_arguments(key, value);
            }
        }

    }

}
