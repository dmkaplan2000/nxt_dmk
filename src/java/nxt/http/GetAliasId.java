package nxt.http;

import nxt.Alias;
import nxt.util.Convert;
import org.json.simple.JSONObject;
import org.json.simple.JSONStreamAware;

import javax.servlet.http.HttpServletRequest;

import static nxt.http.JSONResponses.MISSING_ALIAS;
import static nxt.http.JSONResponses.UNKNOWN_ALIAS;

public final class GetAliasId extends HttpRequestDispatcher.HttpRequestHandler {

    static final GetAliasId instance = new GetAliasId();

    private GetAliasId() {}

    @Override
    JSONStreamAware processRequest(HttpServletRequest req) {

        String alias = req.getParameter("alias");
        if (alias == null) {
            return MISSING_ALIAS;
        }

        Alias aliasData = Alias.getAlias(alias.toLowerCase());
        if (aliasData == null) {
            return UNKNOWN_ALIAS;
        }

        JSONObject response = new JSONObject();
        response.put("id", Convert.convert(aliasData.getId()));
        return response;
    }

}