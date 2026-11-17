# Wave Music — Render Free deployment

Prepared, not deployed. This package creates one Free web service and no databases or persistent disks.

## Setup

1. Create a free Render account at https://dashboard.render.com/register. To remain strictly free, do not add a payment method or choose paid resources.
2. Connect Render in Codex so the Free service can be configured in your account. The source files also need to be available in a Git repository for a standard Blueprint deployment.
3. Put these files at the repository root and create a Blueprint from render.yaml. Review that the only service has plan: free.
4. After deployment, verify the assigned HTTPS /health endpoint. The hostname is accepted automatically using RENDER_EXTERNAL_HOSTNAME.
5. Configure the included wave_music.lua with the deployed HTTPS address and BRIDGE_TOKEN before publishing it. Both values are already supported by the player:

```lua
_G.CalciumAudioBridgeUrl = "https://YOUR_SERVICE.onrender.com"
_G.CalciumAudioBridgeToken = "YOUR_SHARED_ACCESS_KEY"
```

Prefix those assignments to the complete included player. End users run that configured file and do not launch anything on their own devices.

## Free limits

Render sleeps this service after 15 idle minutes; waking can take about a minute. Cached audio disappears on sleep/restart/redeploy. Included usage can run out, and unusually heavy outbound traffic can lead to suspension. Without a payment method, exhausted usage suspends service instead of charging overages. This is a limited test/hobby release, not a guarantee of production uptime or unlimited listeners.

The free profile prepares one song at a time, uses up to eight cached/pending jobs, and limits the completed cache to 128 MiB. The client has a bounded, on-demand cold-start check; it does not send artificial keepalive traffic. The original current player UI is retained. Docker builds and native mobile playback have not been verified locally.

Official references:
- https://render.com/docs/free
- https://render.com/docs/blueprint-spec
- https://render.com/docs/environment-variables
