var _compact = require("lodash.compact");
var _defaults = require("lodash.defaults");
var _filter = require("lodash.filter");
var _first = require("lodash.first");
var _flatten = require("lodash.flatten");
var _forEach = require("lodash.foreach");
var _groupBy = require("lodash.groupby");
var _has = require("lodash.has");
var _intersection = require("lodash.intersection");
var _isEmpty = require("lodash.isempty");
var _isUndefined = require("lodash.isundefined");
var _keyBy = require("lodash.keyby");
var _keys = require("lodash.keys");
var _map = require("lodash.map");
var _sortBy = require("lodash.sortby");
var _take = require("lodash.take");
var _uniq = require("lodash.uniq");
var _values = require("lodash.values");
var _without = require("lodash.without");
var async = require("async");
var flat = require("flat");

module.exports = {

    get_constraints: function(container){
        if(_has(container.tags, "constraints"))
            return _intersection(_keys(container.tags.constraints), _keys(this.list));
        else
            return [];
    },

    enforce: function(core, options, fn){
        var self = this;
        async.eachSeries(this.get_constraints(options.container), function(constraint, fn){
            self.list[constraint].deploy(core, options, fn);
        }, function(err){
            return fn(err, options.available_hosts)
        });
    },

    enforce_remove: function(core, options, fn){
        var self = this;
        async.eachSeries(this.get_constraints(options.application), function(constraint, fn){
            self.list[constraint].remove(core, options, fn);
        }, function(err){
            return fn(err, options.available_containers)
        });
    },

    list: {

        max: {
            harmonize: function(core, options, fn){
                core.cluster.myriad.persistence.keys([core.constants.myriad.CONTAINERS_PREFIX, options.application_name, "*"].join("::"), function(err, containers){
                    var difference = containers.length - options.application.tags.constraints.max;
                    if(difference > 0)
                        core.applications.remove_containers(options.application_name, difference, fn);
                    else
                        return fn();
                });
            },

            deploy: function(core, options, fn){
                var max_containers = options.container.tags.constraints.max;
                core.cluster.myriad.persistence.keys([core.constants.myriad.CONTAINERS_PREFIX, options.application_name, "*"].join("::"), function(err, containers){
                    if((containers.length == max_containers && _isUndefined(options.container.id)) || (containers.length > options.container.tags.constraints.max)){
                        var err = new Error("Max containers already met!");
                        err.fatal = false;
                        return fn(err);
                    }
                    else
                        return fn();
                });
            },

            remove: function(core, options, fn){
                return fn();
            }
        },

        min: {
            harmonize: function(core, options, fn){
                core.cluster.myriad.persistence.keys([core.constants.myriad.CONTAINERS_PREFIX, options.application_name, "*"].join("::"), function(err, containers){
                    var difference = options.application.tags.constraints.min - containers.length;
                    if(difference > 0){
                        async.times(difference, function(index, fn){
                            core.applications.deploy_container(options.application_name, {}, function(){
                                return fn();
                            });
                        }, fn);
                    }
                    else
                        return fn();
                });
            },

            deploy: function(core, options, fn){
                return fn();
            },

            remove: function(core, options, fn){
                var to_remove = options.application.tags.constraints.min;
                if(options.total_containers - options.num_containers < options.application.tags.constraints.min)
                    to_remove = options.total_containers - options.application.tags.constraints.min;

                var available_containers = options.available_containers;

                var available_containers_by_status = _defaults(_groupBy(available_containers, "status"), {
                    loaded: [],
                    loading: [],
                    unloaded: []
                });

                options.available_containers = _take(_flatten([
                    available_containers_by_status.unloaded,
                    available_containers_by_status.loading,
                    available_containers_by_status.loaded
                ]), to_remove);

                return fn();
            }
        },

        per_host: {
            harmonize: function(core, options, fn){
                core.cluster.myriad.persistence.keys([core.constants.myriad.CONTAINERS_PREFIX, options.application_name, "*"].join("::"), function(err, containers){
                    async.map(containers, function(container_name, fn){
                        core.cluster.myriad.persistence.get(container_name, function(err, container){
                            if(err)
                                return fn();
                            else{
                                try{
                                    container = JSON.parse(container);
                                    return fn(err, container);
                                }
                                catch(e){
                                    return fn();
                                }
                            }
                        });
                    }, function(err, containers){
                        containers = _compact(containers);
                        containers = _groupBy(containers, "host");
                        var container_hosts = _keys(containers);

                        var peers = _groupBy(core.cluster.legiond.get_peers(), "mode");
                        var followers = peers.follower || [];

                        async.each(followers, function(follower, fn){
                            var id = follower.id;
                            container_hosts = _without(container_hosts, id);
                            if(_has(containers, id)){
                                var running = containers[id].length;
                                if(running == options.application.tags.constraints.per_host)
                                    return fn();
                                else if(running < options.application.tags.constraints.per_host){
                                    var difference = options.application.tags.constraints.per_host - running;
                                    async.times(difference, function(index, fn){
                                        core.applications.deploy_container(options.application_name, { tags: { host: id } }, function(){
                                            return fn();
                                        });
                                    }, fn);
                                }
                                else{
                                    var difference = running - options.application.tags.constraints.per_host;
                                    async.times(difference, function(index, fn){
                                        core.applications.remove_container(options.application_name, containers[id][index].id, fn);
                                    }, fn);
                                }
                            }
                            else{
                                var difference = options.application.tags.constraints.per_host;
                                async.times(difference, function(index, fn){
                                    core.applications.deploy_container(options.application_name, { tags: { host: id } }, function(){
                                        return fn();
                                    });
                                }, fn);
                            }
                        }, function(){
                            async.each(container_hosts, function(id, fn){
                                async.each(containers[id], function(container, fn){
                                    core.applications.remove_container(options.application_name, container.id, fn);
                                }, function(){
                                    return fn();
                                });
                            }, function(){
                                return fn();
                            });
                        });
                    });
                });
            },

            deploy: function(core, options, fn){
                var hosts = [];

                var desired = options.container.tags.constraints.per_host;

                core.applications.get_containers(options.application_name, function(err, containers){
                    _forEach(options.available_hosts, function(host){
                        var host_containers = _filter(containers, function(container){
                            return container.host = host.id;
                        });

                        host_containers = _groupBy(host_containers, "application");

                        if(!_has(host_containers, options.application_name) || host_containers[options.application_name].length < desired)
                            hosts.push(host);
                    });

                    options.available_hosts = hosts;
                    return fn();
                });
            },

            remove: function(core, options, fn){
                return fn();
            }
        },

        partition: {
            harmonize: function(core, options, fn){
                return fn();
            },

            deploy: function(core, options, fn){
                var peers = _keyBy(core.cluster.legiond.get_peers(), "id");
                var partition = options.container.tags.constraints.partition;

                var hosts = _map(options.available_hosts, function(host){
                    host.tags = flat.flatten(host.tags);
                    return host;
                });

                var possibilities = _groupBy(hosts, function(host){
                    return host.tags[partition];
                });

                var partition_map = {};

                _forEach(_uniq(_keys(possibilities)), function(possiblity){
                    partition_map[possiblity] = 0;
                });

                core.applications.get_containers(options.application_name, function(err, containers){
                    _forEach(containers, function(container){
                        if(container.status != "unloaded"){
                            var peer_tags = flat.flatten(peers[container.host].tags);
                            if(_has(partition_map, peer_tags[partition]))
                                partition_map[peer_tags[partition]]++;
                        }
                    });

                    var partition_val;
                    var min;

                    _forEach(partition_map, function(num_containers, partition_value){
                        if(_isUndefined(min) || num_containers < min){
                            partition_val = partition_value;
                            min = num_containers;
                        }
                    });

                    options.available_hosts = possibilities[partition_val];
                    return fn();
                });
            },

            remove: function(core, options, fn){
                var available_containers = options.available_containers;
                options.available_containers = [];

                var partition = options.application.tags.constraints.partition;

                core.applications.get_containers(options.application.id, function(err, containers){
                    if(err)
                        return fn(err, available_containers);

                    _forEach(containers, function(container){
                        container.tags = flat.flatten(container.tags);
                    });

                    var available_containers_by_partition = _groupBy(containers, function(container){
                        return container.tags[partition];
                    });

                    var available_values = _values(available_containers_by_partition);

                    while(available_values.length > 0){
                        available_values = _sortBy(available_values, function(value){
                            return -value.length;
                        });

                        options.available_containers.push(_first(available_values).shift());
                        if(_isEmpty(_first(available_values)))
                            available_values.shift();
                    }

                    return fn();
                });
            }
        }
    }

}
