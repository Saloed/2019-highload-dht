package ru.mail.polis.service.saloed;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.service.saloed.ClusterTask.ClusterTaskArguments;

public interface ClusterTask<R, D extends ClusterTaskArguments> {

    R processLocal(final D arguments) throws IOException;

    Request preprocessRemote(final Request request, final D arguments) throws IOException;

    Optional<R> obtainRemoteResult(final Response response, final D arguments) throws IOException;

    Response makeResponseForUser(final List<R> data, final D arguments) throws IOException;

    Response makeResponseForService(final R data, final D arguments) throws IOException;

    boolean isLocalTaskForService(final D arguments);

    interface ClusterTaskArguments{

    }
}
