package org.github.datapipeline.core.spark.config;


public class NodeData {

    private String id;

    private String name;

    private String type;

    private NodeConfig config;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public NodeConfig getConfig() {
        return config;
    }

    public void setConfig(NodeConfig config) {
        this.config = config;
    }
}
