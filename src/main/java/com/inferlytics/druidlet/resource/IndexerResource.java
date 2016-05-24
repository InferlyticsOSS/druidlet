/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.resource;

import io.swagger.annotations.Api;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Provides an endpoint to index JSON files
 *
 * @author Sriram
 * @since 4/29/2016
 */

@Path("/indexer/v1/task")
@Api("Druid Indexer API")
public class IndexerResource {
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response indexJson(String json) {
        return Response.ok().build();
    }
}
