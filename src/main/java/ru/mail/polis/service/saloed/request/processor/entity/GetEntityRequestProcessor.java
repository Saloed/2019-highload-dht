package ru.mail.polis.service.saloed.request.processor.entity;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.ResponseInfo;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Collectors;
import one.nio.http.Response;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.timestamp.DAOWithTimestamp;
import ru.mail.polis.dao.timestamp.RecordWithTimestamp;
import ru.mail.polis.service.saloed.request.ResponseUtils;
import ru.mail.polis.service.saloed.request.processor.EntityRequestProcessor;

public class GetEntityRequestProcessor extends
    EntityRequestProcessor {

    public GetEntityRequestProcessor(final DAOWithTimestamp dao, final Arguments arguments) {
        super(dao, arguments);
    }

    @Override
    public Optional<MaybeRecordWithTimestamp> processLocal() {
        try {
            final var record = dao.getRecord(arguments.getKey());
            final var maybe = new MaybeRecordWithTimestamp(record);
            return Optional.of(maybe);
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public BodySubscriber<Optional<MaybeRecordWithTimestamp>> obtainRemoteResult(
        final ResponseInfo response) {
        if (response.statusCode() != 200) {
            return HttpResponse.BodySubscribers.replacing(Optional.empty());
        }
        return HttpResponse.BodySubscribers
            .mapping(HttpResponse.BodySubscribers.ofByteArray(), body -> {
                final var record = RecordWithTimestamp.fromBytes(body);
                return Optional.of(new MaybeRecordWithTimestamp(record));
            });
    }

    @Override
    public Response makeResponseForUser(
        final Collection<MaybeRecordWithTimestamp> data) {
        if (data.size() < arguments.getReplicasAck()) {
            return ResponseUtils.notEnoughReplicas();
        }
        final var notEmptyRecords = data.stream()
            .map(MaybeRecordWithTimestamp::getRecord)
            .filter(it -> !it.isEmpty())
            .collect(Collectors.toList());
        if (notEmptyRecords.isEmpty()) {
            return ResponseUtils.notFound();
        }
        final var lastRecord = notEmptyRecords.stream()
            .max(Comparator.comparingLong(RecordWithTimestamp::getTimestamp))
            .get();
        if (lastRecord.isValue()) {
            final var valueArray = ByteBufferUtils.toArray(lastRecord.getValue());
            return new Response(Response.OK, valueArray);
        }
        return ResponseUtils.notFound();
    }

    @Override
    public HttpRequest.Builder preprocessRemote(final HttpRequest.Builder request) {
        return super.preprocessRemote(request).GET();
    }

    @Override
    public Response makeResponseForService(
        final MaybeRecordWithTimestamp data) {
        return new Response(Response.OK, data.getRecord().toRawBytes());
    }

}
