/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.app;

import com.inferlytics.druidlet.core.DruidIndices;
import com.inferlytics.druidlet.loader.Loader;
import com.inferlytics.druidlet.resource.DruidResource;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.*;
import io.druid.segment.QueryableIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.servlet.DispatcherType;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

/**
 * Can be used to start a standalone instance of Druid or start an embedded instance and query it via REST
 *
 * @author Sriram
 * @since 4/14/2016
 */
public class DruidRunner {
    private Server server;
    private QueryableIndex index;
    private int port;

    /**
     * Create a new DruidRunner to start and stop an embedded Druid instance
     *
     * @param port  Port to listen on
     * @param index Index to make available
     */
    public DruidRunner(int port, QueryableIndex index) {
        this.port = port;
        this.index = index;
    }

    /**
     * Starts the Druid instance and returns the server it is running on
     *
     * @return server hosting Druid
     * @throws Exception
     */
    public Server run() throws Exception {
        if (server == null) {
            server = new Server(port);
            String basePath = "/druid";
            buildSwagger(basePath);
            HandlerList handlers = new HandlerList();
            handlers.addHandler(buildContext(basePath));
            server.setHandler(handlers);
            server.start();
            DruidIndices.getInstance().cache(String.valueOf(port), index);
        } else {
            throw new IllegalStateException("Server already running");
        }
        return server;
    }

    /**
     * Builds the Swagger documentation
     *
     * @param basePath Base path where the resources reside
     */
    private static void buildSwagger(String basePath) {
        // This configures Swagger
        BeanConfig beanConfig = new BeanConfig();
        beanConfig.setVersion("1.0.0");
        beanConfig.setResourcePackage(DruidResource.class.getPackage().getName());
        beanConfig.setBasePath(basePath);
        beanConfig.setDescription("Embedded Druid");
        beanConfig.setTitle("Embedded Druid");
        beanConfig.setScan(true);
    }

    /**
     * Adds resources to the context to be run on the server
     *
     * @param basePath Path to add the resources on
     * @return ContextHandler to add on the server
     */
    private static ContextHandler buildContext(String basePath) {
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.packages(DruidResource.class.getPackage().getName(), ApiListingResource.class.getPackage().getName());
        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder entityBrowser = new ServletHolder(servletContainer);
        ServletContextHandler entityBrowserContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
        entityBrowserContext.setContextPath(basePath);
        entityBrowserContext.addServlet(entityBrowser, "/*");

        FilterHolder corsFilter = new FilterHolder(CrossOriginFilter.class);
        corsFilter.setAsyncSupported(true);
        corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
        corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "*");
        corsFilter.setInitParameter(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, "*");
        corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, "GET,POST,HEAD,DELETE,PUT,OPTIONS");

        entityBrowserContext.addFilter(corsFilter, "/*", EnumSet.allOf(DispatcherType.class));
        return entityBrowserContext;
    }

    /**
     * Runs Druid and blocks until the server is stopped
     *
     * @throws Exception
     */
    public void runBlocking() throws Exception {
        run().join();
    }

    /**
     * Stops the server and removes the index
     *
     * @throws Exception
     */
    public void stop() throws Exception {
        DruidIndices.getInstance().remove(String.valueOf(port));
        server.stop();
    }

    /**
     * Starts the server from the command line
     * Currently indexes nothing, must be modified to index from custom locations
     *
     * @param args Command line arguments
     * @throws Exception Thrown by the server on failure
     */
    public static void main(String[] args) throws Exception {
        // TODO Accept command line parameters to start up Druid
        new DruidRunner(37843, getIndex("test")).run();
    }

    public static QueryableIndex getIndex(String indexKey) throws IOException {
        //  Create druid segments from raw data
        Reader reader = new FileReader(new File("./src/test/resources/report.csv"));

        List<String> columns = Arrays.asList("colo", "pool", "report", "URL", "TS", "metric", "value", "count", "min", "max", "sum");
        List<String> metrics = Arrays.asList("value", "count", "min", "max", "sum");
        List<String> dimensions = new ArrayList<>(columns);
        dimensions.removeAll(metrics);
        Loader loader = Loader.csv(reader, columns, dimensions, "TS");

        DimensionsSpec dimensionsSpec = new DimensionsSpec(dimensions, null, null);
        AggregatorFactory[] metricsAgg = new AggregatorFactory[]{
                new LongSumAggregatorFactory("agg_count", "count"),
                new DoubleMaxAggregatorFactory("agg_max", "max"),
                new DoubleMinAggregatorFactory("agg_min", "min"),
                new DoubleSumAggregatorFactory("agg_sum", "sum")
        };
        IncrementalIndexSchema indexSchema = new IncrementalIndexSchema(0, QueryGranularity.ALL, dimensionsSpec, metricsAgg);
        return DruidIndices.getInstance().cache(indexKey, loader, indexSchema);
    }
}
