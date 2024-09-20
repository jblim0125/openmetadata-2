package org.openmetadata.service.resources.storages;

import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.openmetadata.schema.api.VoteRequest;
import org.openmetadata.schema.api.data.CreateContainer;
import org.openmetadata.schema.api.data.CreateTableProfile;
import org.openmetadata.schema.api.data.RestoreEntity;
import org.openmetadata.schema.api.tests.CreateCustomMetric;
import org.openmetadata.schema.entity.data.Container;
import org.openmetadata.schema.tests.CustomMetric;
import org.openmetadata.schema.type.*;
import org.openmetadata.service.Entity;
import org.openmetadata.service.jdbi3.ContainerRepository;
import org.openmetadata.service.jdbi3.ListFilter;
import org.openmetadata.service.resources.Collection;
import org.openmetadata.service.resources.EntityResource;
import org.openmetadata.service.security.Authorizer;
import org.openmetadata.service.security.policyevaluator.OperationContext;
import org.openmetadata.service.security.policyevaluator.ResourceContext;
import org.openmetadata.service.util.FullyQualifiedName;
import org.openmetadata.service.util.JsonUtils;
import org.openmetadata.service.util.ResultList;

import javax.json.JsonPatch;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.util.List;
import java.util.UUID;

@Path("/v1/containers")
@Tag(
        name = "Containers",
        description =
                "A Container is an abstraction for any path(including the top level eg. bucket in S3) storing "
                        + "data in an Object store such as S3, GCP, Azure. It maps a tree-like structure, where each Container "
                        + "can have a parent and a list of sub-folders, and it can be structured - where it contains structured data, or unstructured where no schema for its data is defined.")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Collection(name = "containers")
public class ContainerResource extends EntityResource<Container, ContainerRepository> {
    public static final String COLLECTION_PATH = "v1/containers/";
    static final String FIELDS =
            "parent,children,dataModel,owner,tags,followers,extension,domain,sourceHash";

    @Override
    public Container addHref(UriInfo uriInfo, Container container) {
        super.addHref(uriInfo, container);
        Entity.withHref(uriInfo, container.getService());
        Entity.withHref(uriInfo, container.getParent());
        return container;
    }

    public ContainerResource(Authorizer authorizer) {
        super(Entity.CONTAINER, authorizer);
    }

    @Override
    protected List<MetadataOperation> getEntitySpecificOperations() {
        addViewOperation("parent,children,dataModel", MetadataOperation.VIEW_BASIC);
        return null;
    }

    public static class ContainerList extends ResultList<Container> {
        /* Required for serde */
    }

    public static class TableProfileList extends ResultList<TableProfile> {
        /* Required for serde */
    }

    public static class ColumnProfileList extends ResultList<ColumnProfile> {
        /* Required for serde */
    }

    public static class SystemProfileList extends ResultList<SystemProfile> {
        /* Required for serde */
    }

    @GET
    @Valid
    @Operation(
            operationId = "listContainers",
            summary = "List Containers",
            description =
                    "Get a list of containers, optionally filtered by `service` it belongs to. Use `fields` "
                            + "parameter to get only necessary fields. Use cursor-based pagination to limit the number "
                            + "entries in the list using `limit` and `before` or `after` query params.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "List of containers",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = ContainerResource.ContainerList.class)))
            })
    public ResultList<Container> list(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(
                    description = "Fields requested in the returned resource",
                    schema = @Schema(type = "string", example = FIELDS))
            @QueryParam("fields")
            String fieldsParam,
            @Parameter(
                    description = "Filter Containers by Object Store Service name",
                    schema = @Schema(type = "string", example = "s3West"))
            @QueryParam("service")
            String service,
            @Parameter(
                    description = "Filter by Containers at the root level. E.g., without parent",
                    schema = @Schema(type = "boolean", example = "true"))
            @QueryParam("root")
            @DefaultValue("false")
            Boolean root,
            @Parameter(description = "Limit the number containers returned. (1 to 1000000, default = 10)")
            @DefaultValue("10")
            @Min(0)
            @Max(1000000)
            @QueryParam("limit")
            int limitParam,
            @Parameter(
                    description = "Returns list of containers before this cursor",
                    schema = @Schema(type = "string"))
            @QueryParam("before")
            String before,
            @Parameter(
                    description = "Returns list of containers after this cursor",
                    schema = @Schema(type = "string"))
            @QueryParam("after")
            String after,
            @Parameter(
                    description = "Include all, deleted, or non-deleted entities.",
                    schema = @Schema(implementation = Include.class))
            @QueryParam("include")
            @DefaultValue("non-deleted")
            Include include) {
        ListFilter filter = new ListFilter(include).addQueryParam("service", service);
        if (root != null) {
            filter.addQueryParam("root", root.toString());
        }
        return super.listInternal(
                uriInfo, securityContext, fieldsParam, filter, limitParam, before, after);
    }

    @GET
    @Path("/{id}")
    @Operation(
            operationId = "getContainerByID",
            summary = "Get an Object Store Container",
            description = "Get an Object Store container by `id`.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "The container",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class))),
                    @ApiResponse(responseCode = "404", description = "Container for instance {id} is not found")
            })
    public Container get(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @PathParam("id") UUID id,
            @Parameter(
                    description = "Fields requested in the returned resource",
                    schema = @Schema(type = "string", example = FIELDS))
            @QueryParam("fields")
            String fieldsParam,
            @Parameter(
                    description = "Include all, deleted, or non-deleted entities.",
                    schema = @Schema(implementation = Include.class))
            @QueryParam("include")
            @DefaultValue("non-deleted")
            Include include) {
        return getInternal(uriInfo, securityContext, id, fieldsParam, include);
    }

    @GET
    @Path("/name/{fqn}")
    @Operation(
            operationId = "getContainerByFQN",
            summary = "Get an Container by name",
            description = "Get an Container by fully qualified name.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "The container",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class))),
                    @ApiResponse(responseCode = "404", description = "Container for instance {id} is not found")
            })
    public Container getByName(
            @Context UriInfo uriInfo,
            @PathParam("fqn") String fqn,
            @Context SecurityContext securityContext,
            @Parameter(
                    description = "Fields requested in the returned resource",
                    schema = @Schema(type = "string", example = FIELDS))
            @QueryParam("fields")
            String fieldsParam,
            @Parameter(
                    description = "Include all, deleted, or non-deleted entities.",
                    schema = @Schema(implementation = Include.class))
            @QueryParam("include")
            @DefaultValue("non-deleted")
            Include include) {
        return getByNameInternal(uriInfo, securityContext, fqn, fieldsParam, include);
    }

    @POST
    @Operation(
            operationId = "createContainer",
            summary = "Create a Container",
            description = "Create a new Container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Container",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class))),
                    @ApiResponse(responseCode = "400", description = "Bad request")
            })
    public Response create(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Valid CreateContainer create) {
        Container container = getContainer(create, securityContext.getUserPrincipal().getName());
        return create(uriInfo, securityContext, container);
    }

    @PATCH
    @Path("/{id}")
    @Operation(
            operationId = "patchContainer",
            summary = "Update a Container",
            description = "Update an existing Container using JsonPatch.",
            externalDocs =
            @ExternalDocumentation(
                    description = "JsonPatch RFC",
                    url = "https://tools.ietf.org/html/rfc6902"))
    @Consumes(MediaType.APPLICATION_JSON_PATCH_JSON)
    public Response patch(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the Container", schema = @Schema(type = "string"))
            @PathParam("id")
            UUID id,
            @RequestBody(
                    description = "JsonPatch with array of operations",
                    content =
                    @Content(
                            mediaType = MediaType.APPLICATION_JSON_PATCH_JSON,
                            examples = {
                                    @ExampleObject("[{op:remove, path:/a},{op:add, path: /b, value: val}]")
                            }))
            JsonPatch patch) {
        return patchInternal(uriInfo, securityContext, id, patch);
    }

    @PATCH
    @Path("/name/{fqn}")
    @Operation(
            operationId = "patchContainer",
            summary = "Update a Container using name.",
            description = "Update an existing Container using JsonPatch.",
            externalDocs =
            @ExternalDocumentation(
                    description = "JsonPatch RFC",
                    url = "https://tools.ietf.org/html/rfc6902"))
    @Consumes(MediaType.APPLICATION_JSON_PATCH_JSON)
    public Response patch(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Name of the Container", schema = @Schema(type = "string"))
            @PathParam("fqn")
            String fqn,
            @RequestBody(
                    description = "JsonPatch with array of operations",
                    content =
                    @Content(
                            mediaType = MediaType.APPLICATION_JSON_PATCH_JSON,
                            examples = {
                                    @ExampleObject("[{op:remove, path:/a},{op:add, path: /b, value: val}]")
                            }))
            JsonPatch patch) {
        return patchInternal(uriInfo, securityContext, fqn, patch);
    }

    @PUT
    @Operation(
            operationId = "createOrUpdateContainer",
            summary = "Create or update a Container",
            description = "Create a new Container, if it does not exist or update an existing container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "The Container",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class))),
                    @ApiResponse(responseCode = "400", description = "Bad request")
            })
    public Response createOrUpdate(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Valid CreateContainer create) {
        Container container = getContainer(create, securityContext.getUserPrincipal().getName());
        return createOrUpdate(uriInfo, securityContext, container);
    }

    @PUT
    @Path("/{id}/sampleData")
    @Operation(
            operationId = "addSampleData",
            summary = "Add sample data",
            description = "Add sample data to the container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Successfully update the Container",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class)))
            })
    public Container addSampleData(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the container",
                    schema = @Schema(type = "UUID")) @PathParam("id")
            UUID id,
            @Valid TableData tableData) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.EDIT_SAMPLE_DATA);
        authorizer.authorize(securityContext, operationContext, getResourceContextById(id));
        Container container = repository.addTableSampleData(id, tableData);
        return addHref(uriInfo, container);
    }

    @GET
    @Path("/{id}/sampleData")
    @Operation(
            operationId = "getSampleData",
            summary = "Get sample data",
            description = "Get sample data from the container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Successfully update the Container",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class)))
            })
    public Container getSampleData(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the container",
                    schema = @Schema(type = "UUID")) @PathParam("id")
            UUID id) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.VIEW_SAMPLE_DATA);
        ResourceContext<?> resourceContext = getResourceContextById(id);
        authorizer.authorize(securityContext, operationContext, resourceContext);
        boolean authorizePII = authorizer.authorizePII(securityContext, resourceContext.getOwner());

        Container container = repository.getSampleData(id, authorizePII);
        return addHref(uriInfo, container);
    }

    @DELETE
    @Path("/{id}/sampleData")
    @Operation(
            operationId = "deleteSampleData",
            summary = "Delete sample data",
            description = "Delete sample data from the container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Successfully update the Container",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class)))
            })
    public Container deleteSampleData(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the container",
                    schema = @Schema(type = "UUID")) @PathParam("id")
            UUID id) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.EDIT_SAMPLE_DATA);
        authorizer.authorize(securityContext, operationContext, getResourceContextById(id));
        Container container = repository.deleteSampleData(id);
        return addHref(uriInfo, container);
    }

    @PUT
    @Path("/{id}/tableProfilerConfig")
    @Operation(
            operationId = "addDataProfilerConfig",
            summary = "Add table profile config",
            description = "Add table profile config to the container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Successfully updated the Container",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class)))
            })
    public Container addDataProfilerConfig(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the container",
                    schema = @Schema(type = "UUID")) @PathParam("id") UUID id,
            @Valid TableProfilerConfig tableProfilerConfig) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.EDIT_DATA_PROFILE);
        authorizer.authorize(securityContext, operationContext, getResourceContextById(id));
        Container container = repository.addTableProfilerConfig(id, tableProfilerConfig);
        return addHref(uriInfo, container);
    }

    @GET
    @Path("/{id}/tableProfilerConfig")
    @Operation(
            operationId = "getDataProfilerConfig",
            summary = "Get table profile config",
            description = "Get table profile config to the container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Successfully updated the Container",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class)))
            })
    public Container getDataProfilerConfig(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the Container",
                    schema = @Schema(type = "UUID")) @PathParam("id") UUID id) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.VIEW_DATA_PROFILE);
        authorizer.authorize(securityContext, operationContext, getResourceContextById(id));
        Container container = repository.find(id, Include.NON_DELETED);
        return addHref(
                uriInfo, container.withTableProfilerConfig(repository.getTableProfilerConfig(container)));
    }

    @DELETE
    @Path("/{id}/tableProfilerConfig")
    @Operation(
            operationId = "delete DataProfilerConfig",
            summary = "Delete table profiler config",
            description = "delete table profile config to the container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Successfully deleted the Table profiler config",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class)))
            })
    public Container deleteDataProfilerConfig(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the container",
                    schema = @Schema(type = "UUID")) @PathParam("id") UUID id) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.EDIT_DATA_PROFILE);
        authorizer.authorize(securityContext, operationContext, getResourceContextById(id));
        Container container = repository.deleteTableProfilerConfig(id);
        return addHref(uriInfo, container);
    }

    @GET
    @Path("/{fqn}/tableProfile/latest")
    @Operation(
            operationId = "Get the latest table and column profile",
            summary = "Get the latest table profile",
            description = "Get the latest table and column profile ",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Table profile and column profile",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class)))
            })
    public Response getLatestTableProfile(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "FQN of the container or column", schema = @Schema(type = "String"))
            @PathParam("fqn")
            String fqn) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.VIEW_DATA_PROFILE);
        ResourceContext<?> resourceContext = getResourceContextByName(fqn);
        authorizer.authorize(securityContext, operationContext, resourceContext);
        boolean authorizePII = authorizer.authorizePII(securityContext, resourceContext.getOwner());

        return Response.status(Response.Status.OK)
                .entity(JsonUtils.pojoToJson(repository.getLatestTableProfile(fqn, authorizePII)))
                .build();
    }

    @GET
    @Path("/{fqn}/tableProfile")
    @Operation(
            operationId = "list Profiles",
            summary = "List of table profiles",
            description =
                    "Get a list of all the table profiles for the given table fqn, optionally filtered by `extension`, `startTs` and `endTs` of the profile. "
                            + "Use cursor-based pagination to limit the number of "
                            + "entries in the list using `limit` and `before` or `after` query params.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "List of table profiles",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = TableProfileList.class)))
            })
    public Response listTableProfiles(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "FQN of the table or column", schema = @Schema(type = "String"))
            @PathParam("fqn")
            String fqn,
            @Parameter(
                    description = "Filter table/column profiles after the given start timestamp",
                    schema = @Schema(type = "number"))
            @QueryParam("startTs")
            Long startTs,
            @Parameter(
                    description = "Filter table/column profiles before the given end timestamp",
                    schema = @Schema(type = "number"))
            @QueryParam("endTs")
            Long endTs) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.VIEW_DATA_PROFILE);
        authorizer.authorize(securityContext, operationContext, getResourceContextByName(fqn));
        return Response.status(Response.Status.OK)
                .entity(JsonUtils.pojoToJson(repository.getTableProfiles(fqn, startTs, endTs)))
                .build();
    }

    @GET
    @Path("/{fqn}/columnProfile")
    @Operation(
            operationId = "list column Profiles",
            summary = "List of column profiles",
            description =
                    "Get a list of all the column profiles for the given container fqn, optionally filtered by `extension`, `startTs` and `endTs` of the profile. "
                            + "Use cursor-based pagination to limit the number of "
                            + "entries in the list using `limit` and `before` or `after` query params.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "List of table profiles",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = ColumnProfileList.class)))
            })
    public ResultList<ColumnProfile> listColumnProfiles(
            @Context SecurityContext securityContext,
            @Parameter(description = "FQN of the column", schema = @Schema(type = "String"))
            @PathParam("fqn")
            String fqn,
            @Parameter(
                    description = "Filter table/column profiles after the given start timestamp",
                    schema = @Schema(type = "number"))
            @NotNull
            @QueryParam("startTs")
            Long startTs,
            @Parameter(
                    description = "Filter table/column profiles before the given end timestamp",
                    schema = @Schema(type = "number"))
            @NotNull
            @QueryParam("endTs")
            Long endTs) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.VIEW_DATA_PROFILE);
        String tableFqn =
                FullyQualifiedName.GetContainerTableFQN(
                        fqn); // get table fqn for the resource context (vs column fqn)
        ResourceContext<?> resourceContext = getResourceContextByName(tableFqn);
        authorizer.authorize(securityContext, operationContext, resourceContext);
        boolean authorizePII = authorizer.authorizePII(securityContext, resourceContext.getOwner());
        return repository.getColumnProfiles(fqn, startTs, endTs, authorizePII);
    }

    @GET
    @Path("/{fqn}/systemProfile")
    @Operation(
            operationId = "list system Profiles",
            summary = "List of system profiles",
            description =
                    "Get a list of all the system profiles for the given table fqn, filtered by `extension`, `startTs` and `endTs` of the profile. "
                            + "Use cursor-based pagination to limit the number of "
                            + "entries in the list using `limit` and `before` or `after` query params.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "List of system profiles",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = SystemProfileList.class)))
            })
    public ResultList<SystemProfile> listSystemProfiles(
            @Context SecurityContext securityContext,
            @Parameter(description = "FQN of the table", schema = @Schema(type = "String"))
            @PathParam("fqn")
            String fqn,
            @Parameter(
                    description = "Filter system profiles after the given start timestamp",
                    schema = @Schema(type = "number"))
            @NotNull
            @QueryParam("startTs")
            Long startTs,
            @Parameter(
                    description = "Filter system profiles before the given end timestamp",
                    schema = @Schema(type = "number"))
            @NotNull
            @QueryParam("endTs")
            Long endTs) {
        return repository.getSystemProfiles(fqn, startTs, endTs);
    }

    @PUT
    @Path("/{id}/tableProfile")
    @Operation(
            operationId = "addDataProfiler",
            summary = "Add table profile data",
            description = "Add table profile data to the container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Successfully updated the Container",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class)))
            })
    public Container addDataProfiler(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the container",
                    schema = @Schema(type = "UUID")) @PathParam("id") UUID id,
            @Valid CreateTableProfile createTableProfile) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.EDIT_DATA_PROFILE);
        authorizer.authorize(securityContext, operationContext, getResourceContextById(id));
        Container container = repository.addTableProfileData(id, createTableProfile);
        return addHref(uriInfo, container);
    }

    @DELETE
    @Path("/{fqn}/{entityType}/{timestamp}/profile")
    @Operation(
            operationId = "deleteDataProfiler",
            summary = "Delete table profile data",
            description = "Delete table profile data to the container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Successfully deleted the Container Profile",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = TableProfile.class)))
            })
    public Response deleteDataProfiler(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "FQN of the table or column", schema = @Schema(type = "String"))
            @PathParam("fqn")
            String fqn,
            @Parameter(
                    description = "type of the entity table or column",
                    schema = @Schema(type = "String"))
            @PathParam("entityType")
            String entityType,
            @Parameter(description = "Timestamp of the table profile", schema = @Schema(type = "long"))
            @PathParam("timestamp")
            Long timestamp) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.EDIT_DATA_PROFILE);
        authorizer.authorize(securityContext, operationContext, getResourceContextByName(fqn));
        repository.deleteTableProfile(fqn, entityType, timestamp);
        return Response.ok().build();
    }

    @PUT
    @Path("/{id}/followers")
    @Operation(
            operationId = "addFollower",
            summary = "Add a follower",
            description = "Add a user identified by `userId` as follower of this container",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "OK",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = ChangeEvent.class))),
                    @ApiResponse(responseCode = "404", description = "container for instance {id} is not found")
            })
    public Response addFollower(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the container", schema = @Schema(type = "UUID"))
            @PathParam("id")
            UUID id,
            @Parameter(
                    description = "Id of the user to be added as follower",
                    schema = @Schema(type = "UUID"))
            UUID userId) {
        return repository
                .addFollower(securityContext.getUserPrincipal().getName(), id, userId)
                .toResponse();
    }

    @PUT
    @Path("/{id}/customMetric")
    @Operation(
            operationId = "addCustomMetric",
            summary = "Add column custom metrics",
            description = "Add column custom metrics.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "OK",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class)))
            })
    public Container addCustomMetric(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the container",
                    schema = @Schema(type = "UUID")) @PathParam("id") UUID id,
            @Valid CreateCustomMetric createCustomMetric) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.EDIT_DATA_PROFILE);
        authorizer.authorize(securityContext, operationContext, getResourceContextById(id));
        CustomMetric customMetric = getCustomMetric(securityContext, createCustomMetric);
        Container container = repository.addCustomMetric(id, customMetric);
        return addHref(uriInfo, container);
    }

    @DELETE
    @Path("/{id}/customMetric/{customMetricName}")
    @Operation(
            operationId = "deleteCustomMetric",
            summary = "Delete custom metric from a container",
            description = "Delete a custom metric from a container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "OK",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class)))
            })
    public Container deleteContainerCustomMetric(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the container", schema = @Schema(type = "UUID")) @PathParam("id")
            UUID id,
            @Parameter(description = "column Test Type", schema = @Schema(type = "string"))
            @PathParam("customMetricName")
            String customMetricName) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.EDIT_TESTS);
        authorizer.authorize(securityContext, operationContext, getResourceContextById(id));
        Container container = repository.deleteCustomMetric(id, null, customMetricName);
        return addHref(uriInfo, container);
    }

    @DELETE
    @Path("/{id}/customMetric/{columnName}/{customMetricName}")
    @Operation(
            operationId = "deleteCustomMetric",
            summary = "Delete custom metric from a column",
            description = "Delete a custom metric from a column.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "OK",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class)))
            })
    public Container deleteColumnCustomMetric(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the container", schema = @Schema(type = "UUID")) @PathParam("id")
            UUID id,
            @Parameter(description = "column of the table", schema = @Schema(type = "string"))
            @PathParam("columnName")
            String columnName,
            @Parameter(description = "column Test Type", schema = @Schema(type = "string"))
            @PathParam("customMetricName")
            String customMetricName) {
        OperationContext operationContext =
                new OperationContext(entityType, MetadataOperation.EDIT_TESTS);
        authorizer.authorize(securityContext, operationContext, getResourceContextById(id));
        Container container = repository.deleteCustomMetric(id, columnName, customMetricName);
        return addHref(uriInfo, container);
    }


    @DELETE
    @Path("/{id}/followers/{userId}")
    @Operation(
            operationId = "deleteFollower",
            summary = "Remove a follower",
            description = "Remove the user identified `userId` as a follower of the container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "OK",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = ChangeEvent.class))),
            })
    public Response deleteFollower(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the container", schema = @Schema(type = "string"))
            @PathParam("id")
            String id,
            @Parameter(
                    description = "Id of the user being removed as follower",
                    schema = @Schema(type = "string"))
            @PathParam("userId")
            String userId) {
        return repository
                .deleteFollower(
                        securityContext.getUserPrincipal().getName(),
                        UUID.fromString(id),
                        UUID.fromString(userId))
                .toResponse();
    }

    @GET
    @Path("/{id}/versions")
    @Operation(
            operationId = "listAllContainerVersion",
            summary = "List Container versions",
            description = "Get a list of all the versions of a container identified by `id`",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "List of Container versions",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = EntityHistory.class)))
            })
    public EntityHistory listVersions(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Container Id", schema = @Schema(type = "string")) @PathParam("id")
            UUID id) {
        return super.listVersionsInternal(securityContext, id);
    }

    @GET
    @Path("/{id}/versions/{version}")
    @Operation(
            operationId = "getSpecificContainerVersion",
            summary = "Get a version of the Container",
            description = "Get a version of the Container by given `id`",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Container",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class))),
                    @ApiResponse(
                            responseCode = "404",
                            description = "Container for instance {id} and version {version} is not found")
            })
    public Container getVersion(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Container Id", schema = @Schema(type = "string")) @PathParam("id")
            UUID id,
            @Parameter(
                    description = "Container version number in the form `major`.`minor`",
                    schema = @Schema(type = "string", example = "0.1 or 1.1"))
            @PathParam("version")
            String version) {
        return super.getVersionInternal(securityContext, id, version);
    }

    @DELETE
    @Path("/{id}")
    @Operation(
            operationId = "deleteContainer",
            summary = "Delete a Container",
            description = "Delete a Container by `id`.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK"),
                    @ApiResponse(responseCode = "404", description = "container for instance {id} is not found")
            })
    public Response delete(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Hard delete the entity. (Default = `false`)")
            @QueryParam("hardDelete")
            @DefaultValue("false")
            boolean hardDelete,
            @Parameter(
                    description = "Recursively delete this entity and it's children. (Default `false`)")
            @QueryParam("recursive")
            @DefaultValue("false")
            boolean recursive,
            @Parameter(description = "Container Id", schema = @Schema(type = "UUID")) @PathParam("id")
            UUID id) {
        return delete(uriInfo, securityContext, id, recursive, hardDelete);
    }

    @PUT
    @Path("/{id}/vote")
    @Operation(
            operationId = "updateVoteForEntity",
            summary = "Update Vote for a Entity",
            description = "Update vote for a Entity",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "OK",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = ChangeEvent.class))),
                    @ApiResponse(responseCode = "404", description = "model for instance {id} is not found")
            })
    public Response updateVote(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Id of the Entity", schema = @Schema(type = "UUID")) @PathParam("id")
            UUID id,
            @Valid VoteRequest request) {
        return repository
                .updateVote(securityContext.getUserPrincipal().getName(), id, request)
                .toResponse();
    }

    @DELETE
    @Path("/name/{fqn}")
    @Operation(
            operationId = "deleteContainerByFQN",
            summary = "Delete a Container by fully qualified name",
            description = "Delete a Container by `fullyQualifiedName`.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "OK"),
                    @ApiResponse(
                            responseCode = "404",
                            description = "container for instance {fqn} is not found")
            })
    public Response delete(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Parameter(description = "Hard delete the entity. (Default = `false`)")
            @QueryParam("hardDelete")
            @DefaultValue("false")
            boolean hardDelete,
            @Parameter(description = "Name of the Container", schema = @Schema(type = "string"))
            @PathParam("fqn")
            String fqn) {
        return deleteByName(uriInfo, securityContext, fqn, false, hardDelete);
    }

    @PUT
    @Path("/restore")
    @Operation(
            operationId = "restore",
            summary = "Restore a soft deleted Container.",
            description = "Restore a soft deleted Container.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Successfully restored the Container ",
                            content =
                            @Content(
                                    mediaType = "application/json",
                                    schema = @Schema(implementation = Container.class)))
            })
    public Response restoreContainer(
            @Context UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Valid RestoreEntity restore) {
        return restoreEntity(uriInfo, securityContext, restore.getId());
    }

    private Container getContainer(CreateContainer create, String user) {
        // jblim
        // 생성 요청에서 들어온 데이터 필드들 중 선택해서 데이터를 넣어주는 부분으로
        // Schema 를 변경했다면 이 부분에서도 설정이 필요하다.
        return repository
                .copy(new Container(), create, user)
                .withService(getEntityReference(Entity.STORAGE_SERVICE, create.getService()))
                .withParent(create.getParent())
                .withDataModel(create.getDataModel())
                .withPrefix(create.getPrefix())
                .withNumberOfObjects(create.getNumberOfObjects())
                .withSize(create.getSize())
                .withFullPath(create.getFullPath())
                .withFileFormats(create.getFileFormats())
                .withSourceUrl(create.getSourceUrl())
                .withTableProfilerConfig(create.getTableProfilerConfig())
                .withRdfs(create.getRdfs())
                .withSourceHash(create.getSourceHash());
    }

    private CustomMetric getCustomMetric(SecurityContext securityContext, CreateCustomMetric create) {
        return new CustomMetric()
                .withId(UUID.randomUUID())
                .withDescription(create.getDescription())
                .withName(create.getName())
                .withColumnName(create.getColumnName())
                .withOwner(create.getOwner())
                .withExpression(create.getExpression())
                .withUpdatedBy(securityContext.getUserPrincipal().getName())
                .withUpdatedAt(System.currentTimeMillis());
    }

}
