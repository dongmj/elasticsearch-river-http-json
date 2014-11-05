package com.tuan.elasticsearch.plugin.river.json;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

import com.tuan.elasticsearch.river.json.JsonRiverModule;

public class JsonRiverPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "json";
    }

    @Override
    public String description() {
        return "River Http JSON Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("json", JsonRiverModule.class);
    }
}
