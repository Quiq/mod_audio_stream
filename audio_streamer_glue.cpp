#include <string>
#include <cstring>
#include "mod_audio_stream.h"
#include "WebSocketClient.h"
#include <switch_json.h>
#include <fstream>
#include <switch_buffer.h>
#include <unordered_map>
#include <unordered_set>
#include <atomic>
#include <memory>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include "base64.h"

struct QueuedMark {
    uint64_t bytes_to_play;
    std::string name;
};

#define FRAME_SIZE_8000 320 /* 1000x0.02 (20ms)= 160 x(16bit= 2 bytes) 320 frame size*/

class AudioStreamer
{
public:
    // Factory
    static std::shared_ptr<AudioStreamer> create(
        const char* uuid, const char* wsUri, responseHandler_t callback, int deflate, int heart_beat,
        bool suppressLog, const char* extra_headers, const char* tls_cafile, const char* tls_keyfile,
        const char* tls_certfile, bool tls_disable_hostname_validation) {

        std::shared_ptr<AudioStreamer> sp(new AudioStreamer(
            uuid, wsUri, callback, deflate, heart_beat,
            suppressLog, extra_headers, tls_cafile, tls_keyfile,
            tls_certfile, tls_disable_hostname_validation
        ));

        sp->bindCallbacks(std::weak_ptr<AudioStreamer>(sp));

        sp->client.connect();

        return sp;
    }

    ~AudioStreamer() = default;

private:
    // Ctor
    AudioStreamer(
        const char* uuid, const char* wsUri, responseHandler_t callback, int deflate, int heart_beat,
        bool suppressLog, const char* extra_headers, const char* tls_cafile, const char* tls_keyfile,
        const char* tls_certfile, bool tls_disable_hostname_validation)
        : m_sessionId(uuid), m_notify(callback), m_suppress_log(suppressLog), m_extra_headers(extra_headers), m_playFile(0)
    {

        WebSocketHeaders hdrs;
        WebSocketTLSOptions tls;

        if (m_extra_headers)
        {
            cJSON *headers_json = cJSON_Parse(m_extra_headers);
            if (headers_json)
            {
                cJSON *iterator = headers_json->child;
                while (iterator)
                {
                    if (iterator->type == cJSON_String && iterator->valuestring != nullptr)
                    {
                        hdrs.set(iterator->string, iterator->valuestring);
                    }
                    iterator = iterator->next;
                }
                cJSON_Delete(headers_json);
            }
        }

        client.setUrl(wsUri);

        // Setup eventual TLS options.
        // tls_cafile may hold the special values
        // NONE, which disables validation and SYSTEM which uses
        // the system CAs bundle
        if (tls_cafile)
        {
            tls.caFile = tls_cafile;
        }

        if (tls_keyfile)
        {
            tls.keyFile = tls_keyfile;
        }

        if (tls_certfile)
        {
            tls.certFile = tls_certfile;
        }

        tls.disableHostnameValidation = tls_disable_hostname_validation;
        client.setTLSOptions(tls);

        // Optional heart beat, sent every xx seconds when there is not any traffic
        // to make sure that load balancers do not kill an idle connection.
        if (heart_beat)
            client.setPingInterval(heart_beat);

        // Per message deflate connection is enabled by default. You can tweak its parameters or disable it
        if (deflate)
            client.enableCompression(false);

        // Set extra headers if any
        if (!hdrs.empty())
            client.setHeaders(hdrs);

    }

    struct ProcessResult {
        switch_bool_t ok = SWITCH_FALSE;
        std::string rewrittenJsonData;
        std::vector<std::string> errors;
    };

    static inline void push_err(ProcessResult& out, const std::string& sid, const std::string& s) {
        out.errors.push_back("(" + sid + ") " + s);
    }

    void bindCallbacks(std::weak_ptr<AudioStreamer> wp) {
        client.setMessageCallback([wp](const std::string& message) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;
            self->eventCallback(MESSAGE, message.c_str());
        });

        client.setOpenCallback([wp]() {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "connected");
            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECT_SUCCESS, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });

        client.setErrorCallback([wp](int code, const std::string& msg) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "error");
            cJSON* message = cJSON_CreateObject();
            cJSON_AddNumberToObject(message, "code", code);
            cJSON_AddStringToObject(message, "error", msg.c_str());
            cJSON_AddItemToObject(root, "message", message);

            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECT_ERROR, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });

        client.setCloseCallback([wp](int code, const std::string& reason) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "disconnected");
            cJSON* message = cJSON_CreateObject();
            cJSON_AddNumberToObject(message, "code", code);
            cJSON_AddStringToObject(message, "reason", reason.c_str());
            cJSON_AddItemToObject(root, "message", message);

            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECTION_DROPPED, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });
    }

    switch_media_bug_t *get_media_bug(switch_core_session_t *session)
    {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        if (!channel)
        {
            return nullptr;
        }
        auto *bug = (switch_media_bug_t *)switch_channel_get_private(channel, MY_BUG_NAME);
        return bug;
    }

    inline void media_bug_close(switch_core_session_t *session)
    {
        auto *bug = get_media_bug(session);
        if (bug)
        {
            auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
            tech_pvt->close_requested = 1;
            switch_core_media_bug_close(&bug, SWITCH_FALSE);
        }
    }

    inline void send_initial_metadata(switch_core_session_t *session)
    {
        auto *bug = get_media_bug(session);
        if (bug)
        {
            auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
            if (tech_pvt && strlen(tech_pvt->initialMetadata) > 0)
            {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                                  "sending initial metadata %s\n", tech_pvt->initialMetadata);
                writeText(tech_pvt->initialMetadata);
            }
        }
    }

    void eventCallback(notifyEvent_t event, const char* message) {
        std::string msg = message ? message : "";

        // processing without holding a session
        ProcessResult pr;
        if (event == MESSAGE) {
            pr = processMessage(msg);
            if (pr.ok == SWITCH_TRUE) {
                msg = pr.rewrittenJsonData; // overwrite only on success
            }
        }

        switch_core_session_t* psession = switch_core_session_locate(m_sessionId.c_str());
        if (!psession) {
            return;
        }

        for (const auto& e : pr.errors) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_ERROR, "%s\n", e.c_str());
        }

        switch (event) {
            case CONNECT_SUCCESS:
                send_initial_metadata(psession);
                m_notify(psession, EVENT_CONNECT, msg.c_str());
                break;

            case CONNECTION_DROPPED:
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO, "connection closed\n");
                m_notify(psession, EVENT_DISCONNECT, msg.c_str());
                break;

            case CONNECT_ERROR:
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO, "connection error\n");
                m_notify(psession, EVENT_ERROR, msg.c_str());
                media_bug_close(psession);
                break;

            case MESSAGE:
                if (pr.ok == SWITCH_TRUE) {
                    m_notify(psession, EVENT_PLAY, msg.c_str());
                } else {
                    bool handled_mark_clear = false;
                    cJSON *json = cJSON_Parse(msg.c_str());
                    if (json) {
                        const char *jsEvent = cJSON_GetObjectCstr(json, "event");
                        if (jsEvent) {
                            if (strcmp(jsEvent, "mark") == 0) {
                                cJSON *markObj = cJSON_GetObjectItem(json, "mark");
                                if (markObj) {
                                    const char *markName = cJSON_GetObjectCstr(markObj, "name");
                                    if (markName) {
                                        auto *bug = get_media_bug(psession);
                                        if (bug) {
                                            auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
                                            if (tech_pvt) {
                                                if (switch_mutex_lock(tech_pvt->write_mutex) == SWITCH_STATUS_SUCCESS) {
                                                    queueMark(markName, tech_pvt->total_bytes_written, tech_pvt->total_bytes_played);
                                                    switch_mutex_unlock(tech_pvt->write_mutex);
                                                }
                                                handled_mark_clear = true;
                                            }
                                        }
                                    }
                                }
                            }
                            else if (strcmp(jsEvent, "clear") == 0) {
                                auto *bug = get_media_bug(psession);
                                if (bug) {
                                    auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
                                    if (tech_pvt) {
                                        if (switch_mutex_lock(tech_pvt->write_mutex) == SWITCH_STATUS_SUCCESS) {
                                            if (tech_pvt->write_sbuffer) {
                                                switch_buffer_zero(tech_pvt->write_sbuffer);
                                            }
                                            tech_pvt->total_bytes_played = tech_pvt->total_bytes_written;
                                            switch_mutex_unlock(tech_pvt->write_mutex);
                                        }
                                        clearMarks();
                                        handled_mark_clear = true;
                                    }
                                }
                            }
                        }
                        cJSON_Delete(json);
                    }

                    if (!handled_mark_clear) {
                        m_notify(psession, EVENT_JSON, msg.c_str());
                    }
                }

                if (!m_suppress_log) {
                    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_DEBUG, "response: %s\n", msg.c_str());
                }
                break;
        }

        switch_core_session_rwunlock(psession);
    }

    ProcessResult processMessage(const std::string& message) {
        ProcessResult out;

        // RAII
        using jsonPtr = std::unique_ptr<cJSON, decltype(&cJSON_Delete)>;
        jsonPtr root(cJSON_Parse(message.c_str()), &cJSON_Delete);
        if (!root) return out;

        const char* jsonType = cJSON_GetObjectCstr(root.get(), "type");
        if (!jsonType || std::strcmp(jsonType, "streamAudio") != 0) {
            return out; // not ours
        }

        cJSON* jsonData = cJSON_GetObjectItem(root.get(), "data");
        if (!jsonData) {
            push_err(out, m_sessionId, "processMessage - no data in streamAudio");
            return out;
        }

        const char* jsAudioDataType = cJSON_GetObjectCstr(jsonData, "audioDataType");
        if (!jsAudioDataType) jsAudioDataType = "";

        jsonPtr jsonAudio(cJSON_DetachItemFromObject(jsonData, "audioData"), &cJSON_Delete);

        if (!jsonAudio) {
            push_err(out, m_sessionId, "processMessage - streamAudio missing 'audioData' field");
            return out;
        }

        if (!cJSON_IsString(jsonAudio.get()) || !jsonAudio->valuestring) {
            push_err(out, m_sessionId, "processMessage - 'audioData' is not a string (expected base64 string)");
            return out;
        }

        // sampleRate (only meaningful for raw)
        int sampleRate = 0;
        if (cJSON* jsonSampleRate = cJSON_GetObjectItem(jsonData, "sampleRate")) {
            sampleRate = jsonSampleRate->valueint;
        }

        // map file type
        std::string fileType;
        if (std::strcmp(jsAudioDataType, "raw") == 0) {
            switch (sampleRate) {
                case 8000:  fileType = ".r8";  break;
                case 16000: fileType = ".r16"; break;
                case 24000: fileType = ".r24"; break;
                case 32000: fileType = ".r32"; break;
                case 48000: fileType = ".r48"; break;
                case 64000: fileType = ".r64"; break;
                default:
                    push_err(out, m_sessionId, "processMessage - unsupported sample rate: " + std::to_string(sampleRate));
                    return out;
            }
        } else if (std::strcmp(jsAudioDataType, "wav") == 0)  fileType = ".wav";
        else if (std::strcmp(jsAudioDataType, "mp3") == 0)   fileType = ".mp3";
        else if (std::strcmp(jsAudioDataType, "ogg") == 0)   fileType = ".ogg";
        else if (std::strcmp(jsAudioDataType, "pcmu") == 0)  fileType = ".pcmu";
        else if (std::strcmp(jsAudioDataType, "pcma") == 0)  fileType = ".pcma";
        else {
            push_err(out, m_sessionId, "processMessage - unsupported audio type: " + std::string(jsAudioDataType));
            return out;
        }

        // base64 decode
        std::string decoded;
        try {
            decoded = base64_decode(jsonAudio->valuestring);
        } catch (const std::exception& e) {
            push_err(out, m_sessionId, "processMessage - base64 decode error: " + std::string(e.what()));
            return out;
        }

        // reserve file index
        int idx = 0;
        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            idx = m_playFile++;
        }

        char filePath[256];
        switch_snprintf(filePath, sizeof(filePath), "%s%s%s_%d.tmp%s",
                        SWITCH_GLOBAL_dirs.temp_dir, SWITCH_PATH_SEPARATOR,
                        m_sessionId.c_str(), idx, fileType.c_str());

        // write file
        {
            std::ofstream f(filePath, std::ios::binary);
            if (!f.is_open()) {
                push_err(out, m_sessionId, std::string("processMessage - failed to open file for write: ") + filePath);
                return out;
            }
            f.write(decoded.data(), static_cast<std::streamsize>(decoded.size()));
            if (!f.good()) {
                push_err(out, m_sessionId, std::string("processMessage - failed writing file: ") + filePath);
                return out;
            }
        }

        // track file for cleanup
        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            m_Files.insert(filePath);
        }

        cJSON_AddItemToObject(jsonData, "file", cJSON_CreateString(filePath));

        // return rewritten jsonData as string
        char* jsonString = cJSON_PrintUnformatted(jsonData);
        if (!jsonString) {
            push_err(out, m_sessionId, "processMessage - cJSON_PrintUnformatted failed");
            return out;
        }

        out.rewrittenJsonData.assign(jsonString);
        std::free(jsonString);
        out.ok = SWITCH_TRUE;
        return out;
    }

    ~AudioStreamer() = default;

    void disconnect()
    {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "disconnecting...\n");
        client.disconnect();
    }

    bool isConnected()
    {
        return client.isConnected();
    }

    void writeBinary(uint8_t *buffer, size_t len)
    {
        if (!this->isConnected())
            return;
        client.sendBinary(buffer, len);
    }

    void writeText(const char *text)
    {
        if (!this->isConnected())
            return;
        client.sendMessage(text, strlen(text));
    }

    void deleteFiles()
    {
        std::vector<std::string> files;
        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            if (m_Files.empty())
                return;

            files.assign(m_Files.begin(), m_Files.end());
            m_Files.clear();
            m_playFile = 0;
        }

        for (const auto& fn : files) {
            ::remove(fn.c_str());
        }
    }

    void markCleanedUp()
    {
        m_cleanedUp.store(true, std::memory_order_release);
        // clear ALL callbacks to prevent dangling calls during cleanup
        client.setMessageCallback({});
        client.setOpenCallback({});
        client.setCloseCallback({});
        client.setErrorCallback({});
    }

    bool isCleanedUp() const
    {
        return m_cleanedUp.load(std::memory_order_acquire);
    }

    void queueMark(const std::string& name, uint64_t bytes_written, uint64_t bytes_played)
    {
        std::lock_guard<std::mutex> lock(m_marksMutex);
        if (bytes_written == bytes_played)
        {
            sendMark(name);
        }
        else
        {
            m_Marks.push({bytes_written, name});
        }
    }

    void checkMarks(uint64_t bytes_played)
    {
        std::lock_guard<std::mutex> lock(m_marksMutex);
        while (!m_Marks.empty() && bytes_played >= m_Marks.front().bytes_to_play)
        {
            sendMark(m_Marks.front().name);
            m_Marks.pop();
        }
    }

    void clearMarks()
    {
        std::lock_guard<std::mutex> lock(m_marksMutex);
        while (!m_Marks.empty())
        {
            sendMark(m_Marks.front().name);
            m_Marks.pop();
        }
    }

private:
    void sendMark(const std::string& name)
    {
        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "event", "mark");
        cJSON_AddStringToObject(root, "stream_id", m_sessionId.c_str());
        char seqStr[32];
        snprintf(seqStr, sizeof(seqStr), "%u", ++m_markSequenceNumber);
        cJSON_AddStringToObject(root, "sequence_number", seqStr);
        cJSON *markObj = cJSON_CreateObject();
        cJSON_AddStringToObject(markObj, "name", name.c_str());
        cJSON_AddItemToObject(root, "mark", markObj);
        char *json_str = cJSON_PrintUnformatted(root);
        writeText(json_str);
        switch_safe_free(json_str);
        cJSON_Delete(root);
    }

    std::string m_sessionId;
    responseHandler_t m_notify;
    WebSocketClient client;
    bool m_suppress_log;
    const char *m_extra_headers;
    int m_playFile;
    std::unordered_set<std::string> m_Files;
    std::atomic<bool> m_cleanedUp{false};
    std::mutex m_stateMutex;
    std::mutex m_marksMutex;
    std::queue<QueuedMark> m_Marks;
    uint32_t m_markSequenceNumber{0};
};

namespace
{

    void *SWITCH_THREAD_FUNC write_frame_thread(switch_thread_t *thread, void *obj)
    {
        switch_core_session_t *session = (switch_core_session_t *)obj;
        switch_channel_t *channel = switch_core_session_get_channel(session);
        if (!channel)
            return NULL;

        auto *bug = (switch_media_bug_t *)switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug)
        {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "no media bug in write frame thread\n");
            return NULL;
        }

        private_t *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt)
        {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "write_frame_thread: missing tech_pvt\n");
            return NULL;
        }

        switch_status_t status = SWITCH_STATUS_FALSE;
        switch_timer_t timer = {0};
        switch_frame_t write_frame = {0};
        switch_codec_t write_codec = {0};
        switch_codec_t *read_codec;

        uint32_t sample_rate = tech_pvt->sampling;
        uint32_t channels = tech_pvt->channels;

        read_codec = switch_core_session_get_read_codec(session);
        if (!read_codec || !read_codec->implementation)
        {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "write_frame_thread: no read codec available, shutting down\n");
            return NULL;
        }

        uint32_t interval = read_codec->implementation->microseconds_per_packet / 1000;
        uint32_t samples = switch_samples_per_packet(sample_rate, interval);
        uint32_t tsamples = read_codec->implementation->actual_samples_per_second;
        uint32_t bytes = samples * 2 * channels;

        if (switch_core_codec_init(&write_codec, "L16", NULL, NULL, sample_rate, interval, channels,
                                   SWITCH_CODEC_FLAG_ENCODE | SWITCH_CODEC_FLAG_DECODE, NULL,
                                   switch_core_session_get_pool(session)) == SWITCH_STATUS_SUCCESS)
        {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                              "Codec Activated L16@%uhz %u channels %dms\n", sample_rate, channels, interval);
        }
        write_frame.codec = &write_codec;
        write_frame.data = switch_core_session_alloc(session, SWITCH_RECOMMENDED_BUFFER_SIZE);
        write_frame.channels = channels;
        write_frame.rate = sample_rate;
        write_frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "started write frame thread with sample rate [%u] interval [%u] samples [%u] tsamples [%u] bytes [%u]\n", sample_rate, interval, samples, tsamples, bytes);

        if (switch_core_timer_init(&timer, "soft", interval, tsamples, NULL) != SWITCH_STATUS_SUCCESS)
        {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Timer Setup Failed. Cannot Start Write Thread\n");
            switch_core_codec_destroy(&write_codec);
            return NULL;
        }

        while (!tech_pvt->close_requested && switch_core_session_running(session))
        {
            if (switch_mutex_trylock(tech_pvt->write_mutex) == SWITCH_STATUS_SUCCESS)
            {
                switch_size_t available = switch_buffer_inuse(tech_pvt->write_sbuffer);
                if (available >= bytes)
                {
                    write_frame.datalen = (uint32_t)switch_buffer_read(tech_pvt->write_sbuffer, write_frame.data, bytes);
                    write_frame.samples = write_frame.datalen / 2 / channels;
                    switch_core_session_write_frame(session, &write_frame, SWITCH_IO_FLAG_NONE, 0);

                    tech_pvt->total_bytes_played += write_frame.datalen;
                    if (tech_pvt->pAudioStreamer)
                    {
                        auto sp_ptr = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
                        if (sp_ptr && *sp_ptr) {
                            (*sp_ptr)->checkMarks(tech_pvt->total_bytes_played);
                        }
                    }
                }
                switch_mutex_unlock(tech_pvt->write_mutex);
            }
            switch_core_timer_next(&timer);
        }

        switch_core_timer_destroy(&timer);
        switch_core_codec_destroy(&write_codec);
        return NULL;
    }

    switch_status_t stream_data_init(private_t *tech_pvt, switch_core_session_t *session, char *wsUri,
                                     uint32_t sampling, int wsSampling, int channels, char *metadata, responseHandler_t responseHandler,
                                     int deflate, int heart_beat, bool suppressLog, int rtp_packets, const char *extra_headers,
                                     const char *tls_cafile, const char *tls_keyfile,
                                     const char *tls_certfile, bool tls_disable_hostname_validation)
    {
        int err; // speex

        switch_memory_pool_t *pool = switch_core_session_get_pool(session);

        memset(tech_pvt, 0, sizeof(private_t));

        strncpy(tech_pvt->sessionId, switch_core_session_get_uuid(session), MAX_SESSION_ID);
        strncpy(tech_pvt->ws_uri, wsUri, MAX_WS_URI);
        tech_pvt->sampling = sampling;
        tech_pvt->wsSampling = wsSampling;
        tech_pvt->responseHandler = responseHandler;
        tech_pvt->rtp_packets = rtp_packets;
        tech_pvt->channels = channels;
        tech_pvt->audio_paused = 0;

        if (metadata)
            strncpy(tech_pvt->initialMetadata, metadata, MAX_METADATA_LEN);

        // size_t buflen = (FRAME_SIZE_8000 * wsSampling / 8000 * channels * 1000 / RTP_PERIOD * BUFFERED_SEC);
        const size_t buflen = (FRAME_SIZE_8000 * wsSampling / 8000 * channels * rtp_packets);

        auto sp = AudioStreamer::create(tech_pvt->sessionId, wsUri, responseHandler, deflate, heart_beat,
                                        suppressLog, extra_headers, tls_cafile, tls_keyfile,
                                        tls_certfile, tls_disable_hostname_validation);

        tech_pvt->pAudioStreamer = new std::shared_ptr<AudioStreamer>(sp);

        switch_mutex_init(&tech_pvt->mutex, SWITCH_MUTEX_NESTED, pool);
        switch_mutex_init(&tech_pvt->write_mutex, SWITCH_MUTEX_NESTED, pool);

        if (switch_buffer_create(pool, &tech_pvt->read_sbuffer, buflen) != SWITCH_STATUS_SUCCESS)
        {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "%s: Error creating switch buffer.\n", tech_pvt->sessionId);
            return SWITCH_STATUS_FALSE;
        }
        if (switch_buffer_create(pool, &tech_pvt->write_sbuffer, buflen) != SWITCH_STATUS_SUCCESS)
        {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "%s: Error creating switch buffer.\n", tech_pvt->sessionId);
            return SWITCH_STATUS_FALSE;
        }

        if (wsSampling != sampling)
        {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) resampling from %u to %u\n", tech_pvt->sessionId, sampling, wsSampling);
            tech_pvt->read_resampler = speex_resampler_init(channels, sampling, wsSampling, SWITCH_RESAMPLE_QUALITY, &err);
            if (0 != err)
            {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Error initializing resampler: %s.\n", speex_resampler_strerror(err));
                return SWITCH_STATUS_FALSE;
            }
            tech_pvt->write_resampler = speex_resampler_init(channels, wsSampling, sampling, SWITCH_RESAMPLE_QUALITY, &err);
            if (0 != err)
            {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Error initializing resampler: %s.\n", speex_resampler_strerror(err));
                return SWITCH_STATUS_FALSE;
            }
        }
        else
        {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) no resampling needed for this call\n", tech_pvt->sessionId);
        }

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) stream_data_init\n", tech_pvt->sessionId);

        return SWITCH_STATUS_SUCCESS;
    }

    void destroy_tech_pvt(private_t *tech_pvt)
    {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "%s destroy_tech_pvt\n", tech_pvt->sessionId);
        if (tech_pvt->read_resampler)
        {
            speex_resampler_destroy(tech_pvt->read_resampler);
            tech_pvt->read_resampler = nullptr;
        }
        if (tech_pvt->mutex)
        {
            switch_mutex_destroy(tech_pvt->mutex);
            tech_pvt->mutex = nullptr;
        }
        if (tech_pvt->write_mutex)
        {
            switch_mutex_destroy(tech_pvt->write_mutex);
            tech_pvt->write_mutex = nullptr;
        }
        if (tech_pvt->read_sbuffer)
        {
            switch_buffer_destroy(&tech_pvt->read_sbuffer);
            tech_pvt->read_sbuffer = nullptr;
        }
        if (tech_pvt->write_sbuffer)
        {
            switch_buffer_destroy(&tech_pvt->write_sbuffer);
            tech_pvt->write_sbuffer = nullptr;
        }
        if (tech_pvt->pAudioStreamer)
        {
            auto *sp_ptr = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            delete sp_ptr;
            tech_pvt->pAudioStreamer = nullptr;
        }
    }

    // finish is removed since cleanup is done via shared_ptr destruction

}

extern "C"
{
    int validate_ws_uri(const char *url, char *wsUri)
    {
        const char *scheme = nullptr;
        const char *hostStart = nullptr;
        const char *hostEnd = nullptr;
        const char *portStart = nullptr;

        // Check scheme
        if (strncmp(url, "ws://", 5) == 0)
        {
            scheme = "ws";
            hostStart = url + 5;
        }
        else if (strncmp(url, "wss://", 6) == 0)
        {
            scheme = "wss";
            hostStart = url + 6;
        }
        else
        {
            return 0;
        }

        // Find host end or port start
        hostEnd = hostStart;
        while (*hostEnd && *hostEnd != ':' && *hostEnd != '/')
        {
            if (!std::isalnum(*hostEnd) && *hostEnd != '-' && *hostEnd != '.')
            {
                return 0;
            }
            ++hostEnd;
        }

        // Check if host is empty
        if (hostStart == hostEnd)
        {
            return 0;
        }

        // Check for port
        if (*hostEnd == ':')
        {
            portStart = hostEnd + 1;
            while (*portStart && *portStart != '/')
            {
                if (!std::isdigit(*portStart))
                {
                    return 0;
                }
                ++portStart;
            }
        }

        // Copy valid URI to wsUri
        std::strncpy(wsUri, url, MAX_WS_URI);
        return 1;
    }

    switch_status_t is_valid_utf8(const char *str)
    {
        switch_status_t status = SWITCH_STATUS_FALSE;
        while (*str)
        {
            if ((*str & 0x80) == 0x00)
            {
                // 1-byte character
                str++;
            }
            else if ((*str & 0xE0) == 0xC0)
            {
                // 2-byte character
                if ((str[1] & 0xC0) != 0x80)
                {
                    return status;
                }
                str += 2;
            }
            else if ((*str & 0xF0) == 0xE0)
            {
                // 3-byte character
                if ((str[1] & 0xC0) != 0x80 || (str[2] & 0xC0) != 0x80)
                {
                    return status;
                }
                str += 3;
            }
            else if ((*str & 0xF8) == 0xF0)
            {
                // 4-byte character
                if ((str[1] & 0xC0) != 0x80 || (str[2] & 0xC0) != 0x80 || (str[3] & 0xC0) != 0x80)
                {
                    return status;
                }
                str += 4;
            }
            else
            {
                // invalid character
                return status;
            }
        }
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_send_text(switch_core_session_t *session, char *text)
    {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t *)switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug)
        {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "stream_session_send_text failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);

        if (!tech_pvt)
            return SWITCH_STATUS_FALSE;

        std::shared_ptr<AudioStreamer> streamer;
        std::vector<std::vector<uint8_t>> pending_send;

        if (switch_mutex_trylock(tech_pvt->mutex) != SWITCH_STATUS_SUCCESS) {
            return SWITCH_TRUE;
        }

        if (!tech_pvt->pAudioStreamer) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_TRUE;
        }

        auto sp_ptr = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
        if (!sp_ptr || !(*sp_ptr)) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_TRUE;
        }

        streamer = *sp_ptr;

        if (nullptr == tech_pvt->read_resampler) {

            uint8_t data_buf[SWITCH_RECOMMENDED_BUFFER_SIZE];
            switch_frame_t frame = {0};
            frame.data = data_buf;
            frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;
            size_t available = switch_buffer_freespace(tech_pvt->read_sbuffer);

            while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
                if (!frame.datalen) {
                    continue;
                }

                if (tech_pvt->rtp_packets == 1) {
                    pending_send.emplace_back((uint8_t*)frame.data, (uint8_t*)frame.data + frame.datalen);
                    continue;
                }

                if (available >= frame.datalen) {
                    switch_buffer_write(tech_pvt->read_sbuffer, static_cast<uint8_t *>(frame.data), frame.datalen);
                }

                if (0 == switch_buffer_freespace(tech_pvt->read_sbuffer)) {
                    switch_size_t inuse = switch_buffer_inuse(tech_pvt->read_sbuffer);
                    if (inuse > 0) {
                        std::vector<uint8_t> tmp(inuse);
                        switch_buffer_read(tech_pvt->read_sbuffer, tmp.data(), inuse);
                        switch_buffer_zero(tech_pvt->read_sbuffer);
                        pending_send.emplace_back(std::move(tmp));
                    }
                }
            }

        } else {

            uint8_t data[SWITCH_RECOMMENDED_BUFFER_SIZE];
            switch_frame_t frame = {};
            frame.data = data;
            frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;
            const size_t available = switch_buffer_freespace(tech_pvt->read_sbuffer);

            while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
                if(!frame.datalen) {
                    continue;
                }

                spx_uint32_t in_len = frame.samples;
                spx_uint32_t out_len = (available / (tech_pvt->channels * sizeof(spx_int16_t)));
                spx_int16_t out[available / sizeof(spx_int16_t)];

                if(tech_pvt->channels == 1) {
                    speex_resampler_process_int(tech_pvt->read_resampler,
                                    0,
                                    (const spx_int16_t *)frame.data,
                                    &in_len,
                                    &out[0],
                                    &out_len);
                } else {
                    speex_resampler_process_interleaved_int(tech_pvt->read_resampler,
                                    (const spx_int16_t *)frame.data,
                                    &in_len,
                                    &out[0],
                                    &out_len);
                }

                if(out_len > 0) {
                    const size_t bytes_written = out_len * tech_pvt->channels * sizeof(spx_int16_t);
                    if (tech_pvt->rtp_packets == 1) { //20ms packet
                        const uint8_t* p = (const uint8_t*)out;
                        pending_send.emplace_back(p, p + bytes_written);
                        continue;
                    }
                    if (bytes_written <= available) {
                        switch_buffer_write(tech_pvt->read_sbuffer, (const uint8_t *)out, bytes_written);
                    }
                }

                if(switch_buffer_freespace(tech_pvt->read_sbuffer) == 0) {
                    switch_size_t inuse = switch_buffer_inuse(tech_pvt->read_sbuffer);
                    if (inuse > 0) {
                        std::vector<uint8_t> tmp(inuse);
                        switch_buffer_read(tech_pvt->read_sbuffer, tmp.data(), inuse);
                        switch_buffer_zero(tech_pvt->read_sbuffer);
                        pending_send.emplace_back(std::move(tmp));
                    }
                }
            }
        }

        switch_mutex_unlock(tech_pvt->mutex);

        if (!streamer || !streamer->isConnected()) return SWITCH_TRUE;

        for (auto &chunk : pending_send) {
            if (!chunk.empty()) {
                streamer->writeBinary(chunk.data(), chunk.size());
            }
        }

        return SWITCH_TRUE;
    }

    switch_status_t stream_session_cleanup(switch_core_session_t *session, char *text, int channelIsClosing)
    {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t *)switch_channel_get_private(channel, MY_BUG_NAME);
        if (bug)
        {
            auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
            char sessionId[MAX_SESSION_ID];
            strcpy(sessionId, tech_pvt->sessionId);

            switch_mutex_lock(tech_pvt->mutex);

            if (tech_pvt->cleanup_started) {
                switch_mutex_unlock(tech_pvt->mutex);
                return SWITCH_STATUS_SUCCESS;
            }
            tech_pvt->cleanup_started = 1;

            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) stream_session_cleanup\n", sessionId);

            tech_pvt->close_requested = 1;

            switch_thread_t *write_thread = tech_pvt->write_thread;
            tech_pvt->write_thread = nullptr;

            switch_channel_set_private(channel, MY_BUG_NAME, nullptr);
            if (!channelIsClosing)
            {
                switch_core_media_bug_remove(session, &bug);
            }

            std::shared_ptr<AudioStreamer>* sp_wrap = nullptr;
            std::shared_ptr<AudioStreamer> streamer;

            sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            tech_pvt->pAudioStreamer = nullptr;

            if (sp_wrap && *sp_wrap) {
                streamer = *sp_wrap;
            }

            switch_mutex_unlock(tech_pvt->mutex);

            if (sp_wrap) {
                delete sp_wrap;
                sp_wrap = nullptr;
            }

            if(streamer) {
                streamer->deleteFiles();
                if (text) streamer->writeText(text);

                streamer->markCleanedUp();
                streamer->disconnect();
            }

            if (write_thread)
            {
                switch_status_t thread_status = SWITCH_STATUS_SUCCESS;
                switch_status_t join_result = switch_thread_join(&thread_status, write_thread);
                if (join_result != SWITCH_STATUS_SUCCESS)
                {
                    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "(%s) stream_session_cleanup: failed to join write thread (%d)\n", sessionId, join_result);
                }
            }

            destroy_tech_pvt(tech_pvt);

            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "(%s) stream_session_cleanup: connection closed\n", sessionId);
            return SWITCH_STATUS_SUCCESS;
        }

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "stream_session_cleanup: no bug - websocket connection already closed\n");
        return SWITCH_STATUS_FALSE;
    }
}
