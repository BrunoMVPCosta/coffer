package com.netty.echo.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

/**
 * DefaultCluster
 */
public class DefaultCluster implements Cluster {

    private final String name;
    private final Node localNode;
    private final Collection<Node> nodes;

    public DefaultCluster(Node localNode) {
        this(UUID.randomUUID().toString(), localNode);
    }

    public DefaultCluster(String name, Node localNode) {
        this.name = name;
        this.localNode = localNode;
        this.nodes = new ArrayList<Node>();
    }

    public String getName() {
        return name;
    }

    public Node getMember() {
        return localNode;
    }

    public Node getMember(String id) {
        if (localNode.getId().equals(id)) {
            return localNode;
        }

        return nodes
            .stream()
            .filter(node -> node.getId().equals(id))
            .findFirst()
            .orElse(null);
    }

    @Override
    public void addMember(Node node) {
        this.nodes.add(node);
    }

    @Override
    public Collection<Node> getMembers() {
        return new ArrayList<Node>(nodes);
    }
}