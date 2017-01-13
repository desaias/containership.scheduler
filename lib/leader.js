var _cloneDeep = require("lodash.clonedeep");
var _defaults = require("lodash.defaults");
var _filter = require("lodash.filter");
var _flatten = require("lodash.flatten");
var _forEach = require("lodash.foreach");
var _groupBy = require("lodash.groupby");
var _has = require("lodash.has");
var _includes = require("lodash.includes");
var _isEmpty = require("lodash.isempty");
var _isNull = require("lodash.isnull");
var _isUndefined = require("lodash.isundefined");
var _keyBy = require("lodash.keyby");
var _keys = require("lodash.keys");
var _last = require("lodash.last");
var _map = require("lodash.map");
var _omit = require("lodash.omit");
var _parseInt = require("lodash.parseint");
var _random = require("lodash.random");
var _sample = require("lodash.sample");
var _take = require("lodash.take");
var async = require("async");
var flat = require("flat");
var constraints = require([__dirname, "constraints"].join("/"));

module.exports = function(core){

    return {

        container: {

            harmonize: function(fn){
                var self = this;

                core.cluster.myriad.persistence.keys(core.constants.myriad.APPLICATIONS, function(err, applications){
                    if(err)
                        return fn();

                    async.each(applications, function(application_name, fn){
                        core.cluster.myriad.persistence.get(application_name, function(err, application){
                            application_name = _last(application_name.split("::"));

                            try{
                                application = JSON.parse(application);
                            }
                            catch(err){
                                return fn(err);
                            }

                            async.each(_keys(application.tags.constraints), function(constraint, fn){
                                constraints.list[constraint].harmonize(core, {
                                    application: application,
                                    application_name: application_name
                                }, fn);
                            }, function(){
                                core.applications.get_containers(application_name, function(err, containers){
                                    async.each(containers, function(container, fn){
                                        // if this container does not have an ID, we need to remove the container
                                        // state from myriad. Let it continue, it will be assigned an ID in the
                                        // core.applications.deploy function when it comes back null later on but
                                        // since its state is removed, it should prevent from container loop happening
                                        // on future harmonizes
                                        if (!container.id) {
                                            var badContainerId = [core.constants.myriad.CONTAINERS_PREFIX, application_name, ''].join(core.constants.myriad.DELIMITER);

                                            return core.cluster.myriad.persistence.delete(badContainerId, function(err) {
                                                if (err) {
                                                    core.loggers["containership.scheduler"].log("error", ["Failed to remove:", badContainerId].join(" "));
                                                    core.loggers["containership.scheduler"].log("error", err && err.message);
                                                }

                                                if(container.status === "loaded" && _isNull(container.host)) {
                                                    core.applications.unload_containers(application_name, fn);
                                                } else {
                                                    return fn();
                                                }
                                            });
                                        } else {
                                            if(container.status === "loaded" && _isNull(container.host)) {
                                                core.applications.unload_containers(application_name, fn);
                                            } else {
                                                return fn();
                                            }
                                        }
                                    }, function(){
                                        containers = _groupBy(containers, "status");
                                        if(_has(containers, "unloaded")){
                                            core.loggers["containership.scheduler"].log("info", ["Attempting to replace", containers.unloaded.length, "containers for", application.id].join(" "));
                                            async.each(containers.unloaded, function(container, fn){
                                                container = _omit(container, ["start_time", "host"]);

                                                core.applications.deploy_container(application.id, container, function(err){
                                                    if(err)
                                                        core.loggers["containership.scheduler"].log("error", err.message);

                                                    return fn();
                                                });
                                            }, fn);
                                        }
                                        else
                                            return fn();
                                    });
                                });
                            });
                        });
                    }, fn);
                });
            },

            deploy: function(application_name, container, fn){
                var place_container = function(container, fn){
                    var cid = [application_name, container.id].join("-");

                    // get available hosts
                    var available_hosts = core.cluster.legiond.get_peers();
                    available_hosts.push(core.cluster.legiond.get_attributes());

                    var resources = {
                        cpus: parseFloat(container.cpus),
                        memory: _parseInt(container.memory)
                    }

                    filter.by_mode(available_hosts, function(err, hosts){
                        if(err){
                            core.loggers["containership.scheduler"].log("debug", [err.message, cid].join(" "));
                            return fn();
                        }

                        filter.by_tag(hosts, _cloneDeep(container.tags), function(err, hosts){
                            if(err){
                                core.loggers["containership.scheduler"].log("debug", [err.message, cid].join(" "));
                                return fn();
                            }

                            filter.by_vacancy(hosts, resources, function(err, hosts){
                                if(err){
                                    core.loggers["containership.scheduler"].log("debug", [err.message, cid].join(" "));
                                    return fn();
                                }

                                filter.by_constraints(hosts, function(err, hosts){
                                    if(err){
                                        core.loggers["containership.scheduler"].log("debug", [err.message, cid].join(" "));
                                        return fn();
                                    }
                                    return fn(null, _sample(hosts));
                                });

                            });
                        });
                    });

                }

                var filter = {
                    // get hosts running in follower mode
                    by_mode: function(filtered_hosts, fn){
                        var hosts = _filter(filtered_hosts, function(host){
                            return host.mode == "follower";
                        });

                        if(_isEmpty(hosts))
                            return fn(new Error("No follower nodes detected when deploying"));
                        else
                            return fn(null, hosts);
                    },

                    // filter hosts by tag
                    by_tag: function(filtered_hosts, tags, fn){
                        delete tags.constraints;
                        delete tags.metadata;
                        tags = flat.flatten(tags);

                        var hosts = _filter(filtered_hosts, function(host){
                            var matched = 0;
                            host.tags = flat.flatten(host.tags);
                            _forEach(tags, function(tag_value, tag_name){
                                if(_has(host.tags, tag_name) && host.tags[tag_name] == tag_value)
                                    matched++;
                            });

                            return matched == _keys(tags).length;
                        });

                        if(_isEmpty(hosts))
                            return fn(new Error("No follower nodes met the given tag criteria when deploying"));
                        else
                            return fn(null, hosts);
                    },

                    // filter hosts by vacancy
                    by_vacancy: function(filtered_hosts, resources, fn){
                        var overhead = 32;
                        var hosts = {};

                        _forEach(filtered_hosts, function(host){
                            hosts[host.id] = host;
                            hosts[host.id].available_cpus = host.cpus;
                            hosts[host.id].available_memory = host.memory;
                        });

                        core.cluster.myriad.persistence.keys([core.constants.myriad.CONTAINERS_PREFIX, "*", "*"].join("::"), function(err, containers){
                            if(err)
                                return fn(new Error("No follower nodes had sufficient resources when deploying"));

                            async.each(containers, function(container_name, fn){
                                core.cluster.myriad.persistence.get(container_name, function(err, container){
                                    if(err)
                                        return fn();

                                    try{
                                        container = JSON.parse(container);
                                        if(_includes(_keys(hosts), container.host)){
                                            hosts[container.host].available_cpus -= parseFloat(container.cpus);
                                            hosts[container.host].available_memory -= (_parseInt(container.memory) + overhead) * (1024 * 1024);
                                        }
                                    }
                                    catch(err){}
                                    return fn();
                                });
                            }, function(){
                                var hosts = _filter(filtered_hosts, function(host){
                                    var available_cpus = host.available_cpus.toFixed(2);
                                    delete host.available_cpus;
                                    var available_memory = host.available_memory / (1024 * 1024);
                                    delete host.available_memory;
                                    available_memory -= overhead;
                                    return (available_memory - resources.memory >= 0) && (available_cpus - resources.cpus >= 0);
                                });

                                if(_isEmpty(hosts))
                                    return fn(new Error("No follower nodes had sufficient resources when deploying"));
                                else
                                    return fn(null, hosts);
                            });
                        });
                    },

                    // filter hosts by constraints
                    by_constraints: function(filtered_hosts, fn){
                        constraints.enforce(core, {
                            available_hosts: filtered_hosts,
                            container: container,
                            application_name: application_name
                        }, function(err, hosts){
                            if(err)
                                return fn(err);
                            else if(_isEmpty(hosts))
                                return fn(new Error("Could not meet constraints when deploying"));
                            else
                                return fn(null, hosts);
                        });
                    }
                }

                var get_host_port = function(host, fn){
                    core.cluster.myriad.persistence.keys(core.constants.myriad.APPLICATIONS, function(err, application_names){
                        if(err)
                            return fn();

                        async.map(application_names, function(application_name, fn){
                            application_name = _last(application_name.split("::"));
                            core.applications.get_containers(application_name, fn);
                        }, function(err, containers){
                            if(err)
                                return fn(err);

                            containers = _flatten(containers);

                            var containers_on_host = _filter(containers, function(container){
                                return container.host == host.id;
                            });

                            var host_port;
                            var utilized_ports = _map(containers_on_host, "host_port");

                            if(container.network_mode == "host" && _has(container, "container_port")){
                                if(_includes(utilized_ports, container.container_port))
                                    return fn(new Error("Requested port already in use"));
                                else{
                                    return fn(null, {
                                        host: host,
                                        host_port: container.container_port
                                    });
                                }
                            }

                            if(_has(container, "host_port") && !_isNull(container.host_port) && !_isUndefined(container.host_port))
                                host_port = _parseInt(container.host_port);

                            if(_isUndefined(host_port)){
                                if(containers_on_host.length >= (core.scheduler.options.container.max_port - core.scheduler.options.container.min_port))
                                    return fn(new Error("No ports left to allocate on desired host"));

                                host_port = _random(core.scheduler.options.container.min_port, core.scheduler.options.container.max_port);
                            }
                            else if(host_port < core.scheduler.options.container.min_port || host_port > core.scheduler.options.container.max_port)
                                return fn(new Error("Invalid port specified"));
                            else if(!_includes(utilized_ports, host_port)){
                                return fn(null, {
                                    host: host,
                                    host_port: host_port
                                });
                            }
                            while(_includes(utilized_ports, host_port))
                                host_port = _random(core.scheduler.options.container.min_port, core.scheduler.options.container.max_port);

                            return fn(null, {
                                host: host,
                                host_port: host_port
                            });
                        });
                    });
                }

                var env_vars = {
                    clear_defaults: function(){
                        _forEach(container.env_vars, function(value, name){
                            if(name.indexOf("CS_") == 0)
                                delete container.env_vars[name];
                        });
                    },

                    set_defaults: function(host, fn){
                        _defaults(container.env_vars, {
                            CS_CONTAINER_ID: container.id,
                            CS_APPLICATION: application_name,
                            CS_CLUSTER_ID: core.cluster_id,
                            CS_FOLLOWER_HOSTNAME: host.host_name
                        });

                        async.series([
                            function(fn){
                                core.cluster.myriad.persistence.keys(core.constants.myriad.APPLICATIONS, function(err, applications){
                                    async.each(applications, function(application_name, fn){
                                        core.cluster.myriad.persistence.get(application_name, function(err, configuration){
                                            if(err)
                                                return fn();

                                            try{
                                                configuration = JSON.parse(configuration);
                                            }
                                            catch(err){
                                                return fn();
                                            }

                                            application_name = _last(application_name.split(core.constants.myriad.DELIMITER));

                                            var name = ["CS", "DISCOVERY", "PORT", application_name.toUpperCase()].join("_");
                                            container.env_vars[name] = configuration.discovery_port;

                                            var name = ["CS", "ADDRESS", application_name.toUpperCase()].join("_");
                                            container.env_vars[name] = [application_name, core.cluster_id, "containership"].join(".");

                                            return fn();
                                        });
                                    }, fn);
                                });
                            },

                            function(fn){
                                core.cluster.myriad.persistence.keys(core.constants.myriad.VARIABLES, function(err, variables){
                                    async.each(variables, function(variable_name, fn){
                                        core.cluster.myriad.persistence.get(variable_name, function(err, value){
                                            if(err)
                                                return fn();

                                            variable_name = _last(variable_name.split(core.constants.myriad.DELIMITER));
                                            var name = ["CS", "MYRIAD", variable_name.toUpperCase()].join("_");
                                            container.env_vars[name] = value;

                                            return fn();
                                        });
                                    }, fn);
                                });
                            }
                        ], fn);
                    }
                }

                // check if application should be deployed to certain host
                if(container.host != null){
                    env_vars.clear_defaults();
                    var hosts = _keyBy(core.cluster.legiond.get_peers(), "id");
                    var host = hosts[container.host];
                    env_vars.set_defaults(host, function(){
                        return get_host_port(host, fn);
                    });
                }

                // get a random host
                else{
                    env_vars.clear_defaults();
                    place_container(container, function(err, host){
                        if(err && err.fatal)
                            return fn(err);
                        else if(_isUndefined(host))
                            return fn();
                        else{
                            env_vars.set_defaults(host, function(){
                                return get_host_port(host, fn);
                            });
                        }
                    });
                }
            }
        },

        application: {

            get_loadbalancer_port: function(port, fn){
                if(!_isUndefined(port))
                    port = _parseInt(port);

                core.cluster.myriad.persistence.keys(core.constants.myriad.APPLICATIONS, function(err, application_names){
                    async.map(application_names, function(application_name, fn){
                        core.cluster.myriad.persistence.get(application_name, function(err, application){
                            try{
                                return fn(null, JSON.parse(application).discovery_port)
                            }
                            catch(err){
                                return fn(err);
                            }
                        });
                    }, function(err, ports){
                        if(_isUndefined(port)){
                            if(ports.length >= (core.scheduler.options.loadbalancer.max_port - core.scheduler.options.loadbalancer.min_port))
                                return fn(new Error("No ports left to allocate"));

                            port = _random(core.scheduler.options.loadbalancer.min_port, core.scheduler.options.loadbalancer.max_port);
                        }
                        else if(port < core.scheduler.options.loadbalancer.min_port || port > core.scheduler.options.loadbalancer.max_port)
                            return fn(new Error("Invalid port specified"));

                        while(_includes(ports, port))
                            port = _random(core.scheduler.options.loadbalancer.min_port, core.scheduler.options.loadbalancer.max_port);

                        return fn(null, port);
                    });
                });
            },

            remove_containers: function(application_name, num_containers, fn){
                var all_containers = [];
                core.cluster.myriad.persistence.keys([core.constants.myriad.CONTAINERS_PREFIX, application_name, "*"].join("::"), function(err, containers){
                    async.each(containers, function(container_id, fn){
                        core.cluster.myriad.persistence.get(container_id, function(err, container){
                            try{
                                container = JSON.parse(container);
                                all_containers.push(container);
                            }
                            catch(err){}
                            return fn();
                        });
                    }, function(err){
                        if(all_containers.length <= num_containers)
                            return fn(null, _map(all_containers, "id"));
                        else{
                            core.cluster.myriad.persistence.get([core.constants.myriad.APPLICATION_PREFIX, application_name].join(core.constants.myriad.DELIMITER), function(err, application){
                                if(err)
                                    return fn(err);

                                try{
                                    application = JSON.parse(application);
                                }
                                catch(err){
                                    return fn(err);
                                }

                                constraints.enforce_remove(core, {
                                    num_containers: num_containers,
                                    total_containers: all_containers.length,
                                    available_containers: all_containers,
                                    application: application
                                }, function(err, available_containers){
                                    if(err)
                                        return fn(err);
                                    else{
                                        if(available_containers.length > num_containers){
                                            var available_containers_by_status = _defaults(_groupBy(available_containers, "status"), {
                                                loaded: [],
                                                loading: [],
                                                unloaded: []
                                            });

                                            available_containers = _take(_flatten([
                                                available_containers_by_status.unloaded,
                                                available_containers_by_status.loading,
                                                available_containers_by_status.loaded
                                            ]), num_containers);
                                        }

                                        return fn(err, _map(available_containers, "id"));
                                    }
                                });
                            });
                        }
                    });
                });
            }

        }

    }

}
