package com.yahoo.container.jdisc.component;

import com.yahoo.component.ComponentId;
import com.yahoo.container.handler.VipStatus;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.container.jdisc.JdiscBindingsConfig;
import com.yahoo.container.jdisc.ThreadedHttpRequestHandler;
import com.yahoo.jdisc.handler.RequestHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;


public class WarmupTest {

    static class TestHandler extends ThreadedHttpRequestHandler implements RequestHandler {
        boolean throwException;
        int nRequests;

        TestHandler(boolean throwException) {
            super(Executors.newSingleThreadExecutor());
            this.throwException = throwException;
            nRequests = 0;
        }

        @Override
        public HttpResponse handle(HttpRequest request) {
            synchronized (this) {
                nRequests++;
            }
            if (throwException)
                throw new RuntimeException("Nah did not work out");

            return new HttpResponse(200) {
                @Override
                public void render(OutputStream outputStream) throws IOException {
                }
            };
        }
    }

    private static VipStatus status = new VipStatus();

    @Test
    public void test_warmup_all_good() {
        Warmup.setInvoked(false);
        RequestHandler handler = new TestHandler(false);
        new Warmup(status, getConfig("https://*/search/"),
                getComponentMap(handler));
        assertThat(((TestHandler) handler).nRequests, is(Warmup.REQUESTS));
       
    }

    @Test
    public void test_no_warmup_if_in_rotation() {
        Warmup.setInvoked(true);
        TestHandler handler = new TestHandler(false);
        new Warmup(status, getConfig("https://*/search/"),
                getComponentMap(handler));
        assertThat(handler.nRequests, is(0));
    }

    @Test
    public void test_exception_during_warmup() {
        Warmup.setInvoked(false);
        TestHandler handler = new TestHandler(true);
        new Warmup(status, getConfig("https://*/search/"),
                getComponentMap(handler));
        assertThat(handler.nRequests, is(Warmup.REQUESTS));
    }

    @Test
    public void test_no_search_binding() {
        Warmup.setInvoked(false);
        status.setInRotation(false);
        TestHandler handler = new TestHandler(false);
        new Warmup(status, getConfig("https://*/somethings/"),
                getComponentMap(handler));
        assertThat(handler.nRequests, is(0));
    }


    JdiscBindingsConfig getConfig(String binding) {
        return new JdiscBindingsConfig(new JdiscBindingsConfig.Builder()
                .handlers("TestHandler", new JdiscBindingsConfig.Handlers.Builder()
                        .serverBindings(binding)
                ));
    }

    Map<ComponentId, RequestHandler> getComponentMap(RequestHandler requestHandler) {
        Map<ComponentId, RequestHandler> componentIdMap = new HashMap<>();
        ComponentId id = new ComponentId("TestHandler");
        componentIdMap.put(id, requestHandler);
        return componentIdMap;
    }

}
