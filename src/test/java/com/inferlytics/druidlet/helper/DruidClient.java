/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.helper;/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

import com.inferlytics.druidlet.util.Utils;
import io.druid.data.input.Row;
import io.druid.query.Query;
import io.druid.query.Result;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.POST;

import java.io.IOException;
import java.util.List;

/**
 * Acts as a client to make calls to Druid
 *
 * @author Sriram
 * @since 4/14/2016
 */
public class DruidClient {
    private interface Druid {
        @POST("v2")
        Call<List<Result>> topN(@Body Query query);

        @POST("v2")
        Call<List<Row>> groupBy(@Body Query query);
    }

    private Druid druid;

    public DruidClient(String apiUrl) {
        druid = new Retrofit.Builder()
                .baseUrl(apiUrl).addConverterFactory(JacksonConverterFactory.create(Utils.JSON_MAPPER))
                .build().create(Druid.class);
    }

    public List<Result> topN(Query query) throws IOException {
        return druid.topN(query).execute().body();
    }

    public List<Row> groupBy(Query query) throws IOException {
        return druid.groupBy(query).execute().body();
    }
}
