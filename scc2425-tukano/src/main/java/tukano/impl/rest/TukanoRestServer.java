package tukano.impl.rest;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import jakarta.ws.rs.core.Application;

import utils.IP;


public class TukanoRestServer extends Application {
	final private static Logger Log = Logger.getLogger(TukanoRestServer.class.getName());

	static final String INETADDR_ANY = "0.0.0.0";
	static String SERVER_BASE_URI = "http://%s:%s/rest";

	public static final int PORT = 8080;

	public static String serverURI;

	private Set<Object> singletons = new HashSet<>();
	private Set<Class<?>> resources = new HashSet<>();

	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s");
	}

	@Override
	public Set<Class<?>> getClasses() {
		return resources;
	}

	@Override
	public Set<Object> getSingletons() {
		return singletons;
	}

	public TukanoRestServer() {
		serverURI = String.format(SERVER_BASE_URI, IP.hostname(), PORT);

		singletons.add(new RestBlobsResource());
		singletons.add(new RestUsersResourceForSQL());
		singletons.add(new RestShortsResourceForSQL());
	}

	public static void main(String[] args) { }
}
