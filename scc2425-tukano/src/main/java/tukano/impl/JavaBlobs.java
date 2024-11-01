package tukano.impl;

import java.util.logging.Logger;

import tukano.api.Blobs;
import tukano.api.Result;
import tukano.impl.rest.TukanoRestServer;
import tukano.impl.storage.AzureClientStorage;
import tukano.impl.storage.BlobStorage;
import utils.Hash;
import utils.Hex;

import static java.lang.String.format;

public class JavaBlobs implements Blobs {

    private static Blobs instance;
    private static Logger Log = Logger.getLogger(JavaBlobs.class.getName());

    public String baseURI;
    private BlobStorage storage;

    synchronized public static Blobs getInstance() {
        if (instance == null)
            instance = new JavaBlobs();
        return instance;
    }

    private JavaBlobs() {
//		storage = new FilesystemStorage();
        storage = new AzureClientStorage();
        baseURI = String.format("%s/%s/", TukanoRestServer.serverURI, Blobs.NAME);
    }

    @Override
    public Result<Void> upload(String blobId, byte[] bytes, String token) {
        Log.info(() -> format("upload : blobId = %s, sha256 = %s, token = %s\n", blobId, Hex.of(Hash.sha256(bytes)), token));

//        ToDo add Validation
//		if (!validBlobId(blobId, token))
//			return error(FORBIDDEN);

        return storage.write(toPath(blobId), bytes);
    }

    @Override
    public Result<byte[]> download(String blobId, String token) {
        Log.info(() -> format("download : blobId = %s, token=%s\n", blobId, token));

//        ToDo add Validation
//		if( ! validBlobId( blobId, token ) )
//			return error(FORBIDDEN);

        return storage.read(toPath(blobId));
    }

    @Override
    public Result<Void> delete(String blobId, String token) {
        Log.info(() -> format("delete : blobId = %s, token=%s\n", blobId, token));

//        ToDo add Validation
//		if( ! validBlobId( blobId, token ) )
//			return error(FORBIDDEN);

        return storage.delete(toPath(blobId));
    }

    @Override
    public Result<Void> deleteAllBlobs(String userId, String token) {
        Log.info(() -> format("deleteAllBlobs : userId = %s, token=%s\n", userId, token));

//        ToDo add Validation
//		if( ! Token.isValid( token, userId ) )
//			return error(FORBIDDEN);

        return storage.delete(toPath(userId));
    }

    private boolean validBlobId(String blobId, String token) {
        return Token.isValid(token, blobId);
    }

    private String toPath(String blobId) {
        return blobId.replace("+", "/");
    }
}
