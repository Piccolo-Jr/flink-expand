package com.piccolojr.flink.connector.gdb.Gremlin.common;

public abstract class GdbGremlinConnectorBuilder<T> {
    protected String contactPoint;
    protected Integer port;
    protected String username;
    protected String password;
    protected boolean enableSsl = false;
    protected String trustStore = "";

    public T withContactPoint(String contactPoint) {
        this.contactPoint = contactPoint;
        return getThis();
    }

    public T withPort(Integer port) {
        this.port = port;
        return getThis();
    }

    public T withUsername(String username) {
        this.username = username;
        return getThis();
    }

    public T withPassword(String password) {
        this.password = password;
        return getThis();
    }

    public T withEnableSsl(boolean enableSsl) {
        this.enableSsl = enableSsl;
        return getThis();
    }

    public T withTrustStore(String trustStore) {
        this.trustStore = trustStore;
        return getThis();
    }

    protected void validate() {
        if (contactPoint == null || contactPoint.equals("")) {
            throw new IllegalArgumentException("No contactPoint was supplied.");
        }
        if (port == null) {
            throw new IllegalArgumentException("No port was supplied.");
        }
        if (username == null || username.equals("")) {
            throw new IllegalArgumentException("No username was supplied.");
        }
        if (password == null || password.equals("")) {
            throw new IllegalArgumentException("No password was supplied.");
        }
    }

    public abstract T getThis();
}
