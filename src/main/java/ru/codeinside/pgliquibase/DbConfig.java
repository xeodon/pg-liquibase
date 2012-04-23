package ru.codeinside.pgliquibase;

final public class DbConfig {
    final public String username;
    final public String password;
    final public String url;

    public DbConfig(String username, String password, String url) {
        if (username == null || password == null || url == null) {
            throw new NullPointerException();
        }
        this.username = username;
        this.password = password;
        this.url = url;
    }
}
