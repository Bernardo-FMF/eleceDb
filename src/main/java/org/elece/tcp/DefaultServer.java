package org.elece.tcp;

import org.elece.config.DbConfig;
import org.elece.exception.*;
import org.elece.thread.DefaultSocketWorker;
import org.elece.thread.ManagedThreadPool;
import org.elece.thread.SocketWorker;

import javax.net.ServerSocketFactory;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class DefaultServer implements Server {
    private final DbConfig config;
    private final DependencyContainer dependencyContainer;
    private final ManagedThreadPool<SocketWorker> managedThreadPool;

    public DefaultServer(DbConfig config) {
        this.config = config;
        this.dependencyContainer = new DependencyContainer(config);
        this.managedThreadPool = new ManagedThreadPool<>(config);
    }

    @Override
    public void start() throws InterruptedTaskException, StorageException, FileChannelException, ServerException {
        ServerSocket serverSocket = null;

        try {
            ServerSocketFactory factory = ServerSocketFactory.getDefault();

            serverSocket = factory.createServerSocket(config.getPort());

            while (managedThreadPool.isRunning()) {
                Socket socket = serverSocket.accept();
                managedThreadPool.execute(new DefaultSocketWorker(socket, dependencyContainer));
            }
        } catch (Exception exception) {
            throw new ServerException(DbError.SERVER_ERROR, String.format("Error while running the server: %s", exception.getMessage()));
        }
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException exception) {
                throw new ServerException(DbError.SERVER_ERROR, String.format("Error while closing the server: %s", exception.getMessage()));
            }
            dependencyContainer.getFileHandlerPoolFactory().getFileHandlerPool().closeAll();
        }
    }
}
