package com.github.topi314.lavasrc.spotify;

import com.github.topi314.lavalyrics.AudioLyricsManager;
import com.github.topi314.lavalyrics.lyrics.AudioLyrics;
import com.github.topi314.lavalyrics.lyrics.BasicAudioLyrics;
import com.github.topi314.lavasearch.AudioSearchManager;
import com.github.topi314.lavasearch.result.AudioSearchResult;
import com.github.topi314.lavasearch.result.BasicAudioSearchResult;
import com.github.topi314.lavasrc.ExtendedAudioPlaylist;
import com.github.topi314.lavasrc.LavaSrcTools;
import com.github.topi314.lavasrc.mirror.DefaultMirroringAudioTrackResolver;
import com.github.topi314.lavasrc.mirror.MirroringAudioSourceManager;
import com.github.topi314.lavasrc.mirror.MirroringAudioTrackResolver;
import com.sedmelluq.discord.lavaplayer.player.AudioPlayerManager;
import com.sedmelluq.discord.lavaplayer.tools.JsonBrowser;
import com.sedmelluq.discord.lavaplayer.tools.io.HttpClientTools;
import com.sedmelluq.discord.lavaplayer.tools.io.HttpConfigurable;
import com.sedmelluq.discord.lavaplayer.tools.io.HttpInterfaceManager;
import com.sedmelluq.discord.lavaplayer.track.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SpotifySourceManager extends MirroringAudioSourceManager implements HttpConfigurable, AudioSearchManager, AudioLyricsManager {

	public static final Pattern URL_PATTERN = Pattern.compile("(https?://)(www\\.)?open\\.spotify\\.com/((?<region>[a-zA-Z-]+)/)?(user/(?<user>[a-zA-Z0-9-_]+)/)?(?<type>track|album|playlist|artist)/(?<identifier>[a-zA-Z0-9-_]+)");
	public static final Pattern RADIO_MIX_QUERY_PATTERN = Pattern.compile("mix:(?<seedType>album|artist|track|isrc):(?<seed>[a-zA-Z0-9-_]+)");
	public static final String SEARCH_PREFIX = "spsearch:";
	public static final String RECOMMENDATIONS_PREFIX = "sprec:";
	public static final String PREVIEW_PREFIX = "spprev:";
	public static final long PREVIEW_LENGTH = 30000;
	public static final String SHARE_URL = "https://spotify.link/";
	public static final int PLAYLIST_MAX_PAGE_ITEMS = 100;
	public static final int ALBUM_MAX_PAGE_ITEMS = 50;
//	public static final String API_BASE = "https://api.spotify.com/v1/";
	public static final String CLIENT_API_BASE = "https://spclient.wg.spotify.com/";
	public static final String PARTNER_API_BASE = "https://api-partner.spotify.com/pathfinder/v2/query";
	public static final Set<AudioSearchResult.Type> SEARCH_TYPES = Set.of(AudioSearchResult.Type.ALBUM, AudioSearchResult.Type.ARTIST, AudioSearchResult.Type.PLAYLIST, AudioSearchResult.Type.TRACK);
	private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.178 Spotify/1.2.65.255 Safari/537.36";
	private static final Logger log = LoggerFactory.getLogger(SpotifySourceManager.class);
	private final HttpInterfaceManager httpInterfaceManager = HttpClientTools.createDefaultThreadLocalManager();
	private final SpotifyTokenTracker tokenTracker;
	private final String countryCode;
	private int playlistPageLimit = 6;
	private int albumPageLimit = 6;
	private boolean localFiles;
	private boolean resolveArtistsInSearch = true;
	private boolean preferAnonymousToken = false;


	public SpotifySourceManager(String[] providers, String clientId, String clientSecret, String countryCode, AudioPlayerManager audioPlayerManager) {
		this(clientId, clientSecret, null, countryCode, unused -> audioPlayerManager, new DefaultMirroringAudioTrackResolver(providers));
	}

	public SpotifySourceManager(String[] providers, String clientId, String clientSecret, String countryCode, Function<Void, AudioPlayerManager> audioPlayerManager) {
		this(clientId, clientSecret, null, countryCode, audioPlayerManager, new DefaultMirroringAudioTrackResolver(providers));
	}

	public SpotifySourceManager(String clientId, String clientSecret, String countryCode, AudioPlayerManager audioPlayerManager, MirroringAudioTrackResolver mirroringAudioTrackResolver) {
		this(clientId, clientSecret, null, countryCode, unused -> audioPlayerManager, mirroringAudioTrackResolver);
	}

	public SpotifySourceManager(String clientId, String clientSecret, String countryCode, Function<Void, AudioPlayerManager> audioPlayerManager, MirroringAudioTrackResolver mirroringAudioTrackResolver) {
		this(clientId, clientSecret, null, countryCode, audioPlayerManager, mirroringAudioTrackResolver);
	}

	public SpotifySourceManager(String clientId, String clientSecret, String spDc, String countryCode, Function<Void, AudioPlayerManager> audioPlayerManager, MirroringAudioTrackResolver mirroringAudioTrackResolver) {
		this(clientId, clientSecret, false, spDc, countryCode, audioPlayerManager, mirroringAudioTrackResolver);
	}

	public SpotifySourceManager(String clientId, String clientSecret, boolean preferAnonymousToken, String spDc, String countryCode, Function<Void, AudioPlayerManager> audioPlayerManager, MirroringAudioTrackResolver mirroringAudioTrackResolver) {
		this(clientId, clientSecret, preferAnonymousToken, null, spDc, countryCode, audioPlayerManager, mirroringAudioTrackResolver);
	}

	public SpotifySourceManager(String clientId, String clientSecret, boolean preferAnonymousToken, String customTokenEndpoint, String spDc, String countryCode, Function<Void, AudioPlayerManager> audioPlayerManager, MirroringAudioTrackResolver mirroringAudioTrackResolver) {
		super(audioPlayerManager, mirroringAudioTrackResolver);

		this.tokenTracker = new SpotifyTokenTracker(this, clientId, clientSecret, spDc, customTokenEndpoint);

		if (countryCode == null || countryCode.isEmpty()) {
			countryCode = "US";
		}
		this.countryCode = countryCode;
		this.preferAnonymousToken = preferAnonymousToken;
	}

	public void setPlaylistPageLimit(int playlistPageLimit) {
		this.playlistPageLimit = playlistPageLimit;
	}

	public void setAlbumPageLimit(int albumPageLimit) {
		this.albumPageLimit = albumPageLimit;
	}

	public void setLocalFiles(boolean localFiles) {
		this.localFiles = localFiles;
	}

	public void setResolveArtistsInSearch(boolean resolveArtistsInSearch) {
		this.resolveArtistsInSearch = resolveArtistsInSearch;
	}

	public void setClientIDSecret(String clientId, String clientSecret) {
		this.tokenTracker.setClientIDS(clientId, clientSecret);
	}

	public void setSpDc(String spDc) {
		this.tokenTracker.setSpDc(spDc);
	}

	public void setPreferAnonymousToken(boolean preferAnonymousToken) {
		this.preferAnonymousToken = preferAnonymousToken;
	}

	public void setCustomTokenEndpoint(String customTokenEndpoint) {
		this.tokenTracker.setCustomTokenEndpoint(customTokenEndpoint);
	}

	@NotNull
	@Override
	public String getSourceName() {
		return "spotify";
	}

	@Override
	@Nullable
	public AudioLyrics loadLyrics(@NotNull AudioTrack audioTrack) {
		var spotifyTackId = "";
		if (audioTrack instanceof SpotifyAudioTrack) {
			spotifyTackId = audioTrack.getIdentifier();
		}

		if (spotifyTackId.isEmpty()) {
			AudioItem item = AudioReference.NO_TRACK;
			try {
				if (audioTrack.getInfo().isrc != null && !audioTrack.getInfo().isrc.isEmpty()) {
					item = this.getSearch("isrc:" + audioTrack.getInfo().isrc, false);
				}
				if (item == AudioReference.NO_TRACK) {
					item = this.getSearch(String.format("%s %s", audioTrack.getInfo().title, audioTrack.getInfo().author), false);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			if (item == AudioReference.NO_TRACK) {
				return null;
			}
			if (item instanceof AudioTrack) {
				spotifyTackId = ((AudioTrack) item).getIdentifier();
			} else if (item instanceof AudioPlaylist) {
				var playlist = (AudioPlaylist) item;
				if (!playlist.getTracks().isEmpty()) {
					spotifyTackId = playlist.getTracks().get(0).getIdentifier();
				}
			}
		}

		try {
			return this.getLyrics(spotifyTackId);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public AudioLyrics getLyrics(String id) throws IOException {
		if (!this.tokenTracker.hasValidAccountCredentials()) {
			throw new IllegalArgumentException("Spotify spDc must be set");
		}

		var request = new HttpGet(CLIENT_API_BASE + "color-lyrics/v2/track/" + id + "?format=json&vocalRemoval=false");
		request.setHeader("User-Agent", USER_AGENT);
		request.setHeader("App-Platform", "WebPlayer");
		request.setHeader("Authorization", "Bearer " + this.tokenTracker.getAccountAccessToken());
		var json = LavaSrcTools.fetchResponseAsJson(this.httpInterfaceManager.getInterface(), request);
		if (json == null) {
			return null;
		}

		var lyrics = new ArrayList<AudioLyrics.Line>();
		for (var line : json.get("lyrics").get("lines").values()) {
			lyrics.add(new BasicAudioLyrics.BasicLine(
				Duration.ofMillis(line.get("startTimeMs").asLong(0)),
				null,
				line.get("words").text()
			));
		}

		return new BasicAudioLyrics("spotify", json.get("lyrics").get("providerDisplayName").textOrDefault("MusixMatch"), null, lyrics);
	}

	@Override
	public AudioTrack decodeTrack(AudioTrackInfo trackInfo, DataInput input) throws IOException {
		var extendedAudioTrackInfo = super.decodeTrack(input);
		return new SpotifyAudioTrack(trackInfo,
			extendedAudioTrackInfo.albumName,
			extendedAudioTrackInfo.albumUrl,
			extendedAudioTrackInfo.artistUrl,
			extendedAudioTrackInfo.artistArtworkUrl,
			extendedAudioTrackInfo.previewUrl,
			extendedAudioTrackInfo.isPreview,
			this
		);
	}

	@Override
	@Nullable
	public AudioSearchResult loadSearch(@NotNull String query, @NotNull Set<AudioSearchResult.Type> types) {
		try {
			if (query.startsWith(SEARCH_PREFIX)) {
				return this.getAutocomplete(query.substring(SEARCH_PREFIX.length()), types);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	@Override
	public AudioItem loadItem(AudioPlayerManager manager, AudioReference reference) {
		var identifier = reference.identifier;
		var preview = reference.identifier.startsWith(PREVIEW_PREFIX);
		return this.loadItem(preview ? identifier.substring(PREVIEW_PREFIX.length()) : identifier, preview);
	}

	public AudioItem loadItem(String identifier, boolean preview) {
		try {
			if (identifier.startsWith(SEARCH_PREFIX)) {
				return this.getSearch(identifier.substring(SEARCH_PREFIX.length()).trim(), preview);
			}

			if (identifier.startsWith(RECOMMENDATIONS_PREFIX)) {
				return this.getRecommendations(identifier.substring(RECOMMENDATIONS_PREFIX.length()).trim(), preview);
			}

			// If the identifier is a share URL, we need to follow the redirect to find out the real url behind it
			if (identifier.startsWith(SHARE_URL)) {
				var request = new HttpHead(identifier);
				request.setConfig(RequestConfig.custom().setRedirectsEnabled(false).build());
				try (var response = this.httpInterfaceManager.getInterface().execute(request)) {
					if (response.getStatusLine().getStatusCode() == 307) {
						var location = response.getFirstHeader("Location").getValue();
						if (location.startsWith("https://open.spotify.com/")) {
							return this.loadItem(location, preview);
						}
					}
					return null;
				}
			}

			var matcher = URL_PATTERN.matcher(identifier);
			if (!matcher.find()) {
				return null;
			}

			var id = matcher.group("identifier");
			switch (matcher.group("type")) {
				case "album":
					return this.getAlbum(id, preview);

				case "track":
					return this.getTrack(id, preview);

				case "playlist":
					return this.getPlaylist(id, preview);

				case "artist":
					return this.getArtist(id, preview);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	public JsonBrowser getJson(String uri, boolean anonymous, boolean preferAnonymous) throws IOException {
		var request = new HttpGet(uri);
		var accessToken = anonymous ? this.tokenTracker.getAnonymousAccessToken() : this.tokenTracker.getAccessToken(preferAnonymous);
		request.addHeader("Authorization", "Bearer " + accessToken);
		return LavaSrcTools.fetchResponseAsJson(this.httpInterfaceManager.getInterface(), request);
	}

	private AudioSearchResult getAutocomplete(String query, Set<AudioSearchResult.Type> types) throws IOException {
		if (types.isEmpty()) {
			types = SEARCH_TYPES;
		}

		var page = this.getPartnerSearchJson(query, 20, this.countryCode);
		if (page == null) {
			return AudioSearchResult.EMPTY;
		}

		var searchData = this.getPartnerSearchData(page);
		if (searchData.isNull()) {
			return AudioSearchResult.EMPTY;
		}

		var albums = new ArrayList<AudioPlaylist>();
		if (types.contains(AudioSearchResult.Type.ALBUM)) {
			for (var item : this.getPartnerSearchItems(searchData, "albums").values()) {
				var data = this.getPartnerDataNode(item);
				if (data.isNull()) {
					continue;
				}

				albums.add(new SpotifyAudioPlaylist(
					data.get("name").safeText(),
					Collections.emptyList(),
					ExtendedAudioPlaylist.Type.ALBUM,
					this.toOpenSpotifyUrl(data.get("uri").text()),
					data.get("coverArt").get("sources").index(0).get("url").text(),
					data.get("artists").get("items").index(0).get("profile").get("name").text(),
					(int) data.get("trackCount").asLong(0)
				));
			}
		}

		var artists = new ArrayList<AudioPlaylist>();
		if (types.contains(AudioSearchResult.Type.ARTIST)) {
			for (var item : this.getPartnerSearchItems(searchData, "artists").values()) {
				var data = this.getPartnerDataNode(item);
				if (data.isNull()) {
					continue;
				}

				var name = data.get("profile").get("name").safeText();
				artists.add(new SpotifyAudioPlaylist(
					name + "'s Top Tracks",
					Collections.emptyList(),
					ExtendedAudioPlaylist.Type.ARTIST,
					this.toOpenSpotifyUrl(data.get("uri").text()),
					data.get("visuals").get("avatarImage").get("sources").index(0).get("url").text(),
					name,
					null
				));
			}
		}

		var playlists = new ArrayList<AudioPlaylist>();
		if (types.contains(AudioSearchResult.Type.PLAYLIST)) {
			for (var item : this.getPartnerSearchItems(searchData, "playlists").values()) {
				var data = this.getPartnerDataNode(item);
				if (data.isNull()) {
					continue;
				}

				playlists.add(new SpotifyAudioPlaylist(
					data.get("name").safeText(),
					Collections.emptyList(),
					ExtendedAudioPlaylist.Type.PLAYLIST,
					data.get("shareUrl").text() != null ? data.get("shareUrl").text() : this.toOpenSpotifyUrl(data.get("uri").text()),
					data.get("images").get("items").index(0).get("sources").index(0).get("url").text(),
					data.get("ownerV2").get("data").get("name").text(),
					(int) data.get("content").get("totalCount").asLong(0)
				));
			}
		}

		var tracks = new ArrayList<AudioTrack>();
		if (types.contains(AudioSearchResult.Type.TRACK)) {
			tracks.addAll(this.parsePartnerTrackCollection(this.getPartnerSearchItems(searchData, "tracks"), false));
		}

		if (tracks.isEmpty() && albums.isEmpty() && artists.isEmpty() && playlists.isEmpty()) {
			return AudioSearchResult.EMPTY;
		}

		return new BasicAudioSearchResult(tracks, albums, artists, playlists, new ArrayList<>());
	}

	public AudioItem getSearch(String query, boolean preview) throws IOException {
		var page = this.getPartnerSearchJson(query, 50, this.countryCode);
		if (page == null) {
			return AudioReference.NO_TRACK;
		}

		var searchData = this.getPartnerSearchData(page);
		if (searchData.isNull()) {
			return AudioReference.NO_TRACK;
		}

		var tracks = this.parsePartnerTrackCollection(this.getPartnerSearchItems(searchData, "tracks"), preview);
		if (tracks.isEmpty()) {
			return AudioReference.NO_TRACK;
		}

		return new BasicAudioPlaylist("Spotify Search: " + query, tracks, null, true);
	}

	public AudioItem getRecommendations(String query, boolean preview) throws IOException {
		Matcher matcher = RADIO_MIX_QUERY_PATTERN.matcher(query);
		if (matcher.find()) {
			String seedType = matcher.group("seedType");
			String seed = matcher.group("seed");
			if (seedType.equals("isrc")) {
				AudioItem item = this.getSearch("isrc:" + seed, preview);
				if (item == AudioReference.NO_TRACK) {
					return AudioReference.NO_TRACK;
				}
				if (item instanceof AudioTrack) {
					seed = ((AudioTrack) item).getIdentifier();
					seedType = "track";
				} else if (item instanceof AudioPlaylist) {
					var playlist = (AudioPlaylist) item;
					if (!playlist.getTracks().isEmpty()) {
						seed = playlist.getTracks().get(0).getIdentifier();
						seedType = "track";
					} else {
						return AudioReference.NO_TRACK;
					}
				}
			}
			JsonBrowser rjson = this.getJson(CLIENT_API_BASE + "inspiredby-mix/v2/seed_to_playlist/spotify:" + seedType + ":" + seed + "?response-format=json", true, this.preferAnonymousToken);
			JsonBrowser mediaItems = rjson.get("mediaItems");
			if (mediaItems.isList() && !mediaItems.values().isEmpty()) {
				String playlistId = mediaItems.index(0).get("uri").text().split(":")[2];
				return this.getPlaylist(playlistId, preview);
			}

		}

		var seedTrackId = this.getQueryValue(query);
		if (seedTrackId == null || seedTrackId.isBlank()) {
			return AudioReference.NO_TRACK;
		}

		var page = this.getPartnerRecommendationsJson(seedTrackId, this.countryCode);
		if (page == null) {
			return AudioReference.NO_TRACK;
		}

		var data = page.get("data");
		var items = this.firstNonNull(
			data.get("internalLinkRecommenderTrack").get("tracks").get("items"),
			data.get("internalLinkRecommenderTrack").get("items"),
			data.get("recommendations").get("items")
		);

		if (items.isNull()) {
			return AudioReference.NO_TRACK;
		}

		var tracks = this.parsePartnerTrackCollection(items, preview);
		if (tracks.isEmpty()) {
			return AudioReference.NO_TRACK;
		}

		return new SpotifyAudioPlaylist("Spotify Recommendations:", tracks, ExtendedAudioPlaylist.Type.RECOMMENDATIONS, null, null, null, null);
	}

	public AudioItem getAlbum(String id, boolean preview) throws IOException {
		var tracks = new ArrayList<AudioTrack>();
		var albumName = "";
		var albumUrl = "https://open.spotify.com/album/" + id;
		String artworkUrl = null;
		String author = null;
		var totalTracks = 0;

		JsonBrowser page;
		var offset = 0;
		var pages = 0;
		do {
			page = this.getPartnerAlbumJson(id, offset, this.countryCode);
			if (page == null) {
				if (offset == 0) {
					return AudioReference.NO_TRACK;
				}
				break;
			}

			var album = this.getPartnerAlbumData(page);
			if (album.isNull()) {
				if (offset == 0) {
					return AudioReference.NO_TRACK;
				}
				break;
			}

			if (offset == 0) {
				albumName = album.get("name").safeText();
				var shareUrl = album.get("shareUrl").text();
				if (shareUrl != null && !shareUrl.isBlank()) {
					albumUrl = shareUrl;
				}
				artworkUrl = album.get("coverArt").get("sources").index(0).get("url").text();
				author = album.get("artists").get("items").index(0).get("profile").get("name").text();
			}

			var trackItems = this.firstNonNull(
				album.get("tracks").get("items"),
				album.get("trackItems").get("items")
			);
			if (trackItems.isNull()) {
				break;
			}

			totalTracks = (int) this.firstNonNull(
				album.get("tracks").get("totalCount"),
				album.get("trackCount")
			).asLong(totalTracks);
			offset += ALBUM_MAX_PAGE_ITEMS;
			tracks.addAll(this.parsePartnerTrackCollection(trackItems, preview));
		}
		while (offset < totalTracks && ++pages < this.albumPageLimit);

		if (tracks.isEmpty()) {
			return AudioReference.NO_TRACK;
		}

		return new SpotifyAudioPlaylist(albumName, tracks, ExtendedAudioPlaylist.Type.ALBUM, albumUrl, artworkUrl, author, totalTracks > 0 ? totalTracks : tracks.size());

	}

	public AudioItem getPlaylist(String id, boolean preview) throws IOException {
		var tracks = new ArrayList<AudioTrack>();
		var playlistName = "";
		var playlistUrl = "https://open.spotify.com/playlist/" + id;
		String artworkUrl = null;
		String author = null;
		var totalTracks = 0;

		JsonBrowser page;
		var offset = 0;
		var pages = 0;
		do {
			page = this.getPartnerPlaylistJson(id, offset, this.countryCode);
			if (page == null) {
				if (offset == 0) {
					return AudioReference.NO_TRACK;
				}
				break;
			}

			var playlist = page.get("data").get("playlistV2");
			if (playlist.isNull()) {
				if (offset == 0) {
					return AudioReference.NO_TRACK;
				}
				break;
			}

			if (offset == 0) {
				playlistName = playlist.get("name").safeText();
				playlistUrl = playlist.get("shareUrl").text() != null ? playlist.get("shareUrl").text() : playlistUrl;
				artworkUrl = playlist.get("images").get("items").index(0).get("sources").index(0).get("url").text();
				author = playlist.get("ownerV2").get("data").get("name").text();
			}

			totalTracks = (int) playlist.get("content").get("totalCount").asLong(totalTracks);
			offset += PLAYLIST_MAX_PAGE_ITEMS;

			for (var value : playlist.get("content").get("items").values()) {
				var track = value.get("itemV2").get("data");
				if (track.isNull()) {
					continue;
				}

				if (track.get("__typename").safeText().equals("Episode") || track.get("mediaType").safeText().equals("AUDIOBOOK_CHAPTER")) {
					continue;
				}

				if (!this.localFiles && track.get("uri").safeText().startsWith("spotify:local:")) {
					continue;
				}

				tracks.add(this.parsePartnerTrack(track, preview));
			}

		}
		while (offset < totalTracks && ++pages < this.playlistPageLimit);

		return new SpotifyAudioPlaylist(playlistName, tracks, ExtendedAudioPlaylist.Type.PLAYLIST, playlistUrl, artworkUrl, author, totalTracks);
	}

	public AudioItem getArtist(String id, boolean preview) throws IOException {
		var page = this.getPartnerArtistJson(id, this.countryCode);
		if (page == null) {
			return AudioReference.NO_TRACK;
		}

		var artist = this.getPartnerArtistData(page);
		if (artist.isNull()) {
			return AudioReference.NO_TRACK;
		}

		var topTrackItems = this.firstNonNull(
			artist.get("discography").get("topTracks").get("items"),
			artist.get("topTracks").get("items"),
			page.get("data").get("artistUnion").get("topTracks").get("items")
		);
		if (topTrackItems.isNull()) {
			return AudioReference.NO_TRACK;
		}

		var tracks = this.parsePartnerTrackCollection(topTrackItems, preview);
		if (tracks.isEmpty()) {
			return AudioReference.NO_TRACK;
		}

		var name = artist.get("profile").get("name").safeText();
		var artistUrl = this.toOpenSpotifyUrl(artist.get("uri").text());
		var artwork = artist.get("visuals").get("avatarImage").get("sources").index(0).get("url").text();

		return new SpotifyAudioPlaylist(name + "'s Top Tracks", tracks, ExtendedAudioPlaylist.Type.ARTIST, artistUrl, artwork, name, tracks.size());
	}

	public AudioItem getTrack(String id, boolean preview) throws IOException {
		var page = this.getPartnerTrackJson(id, this.countryCode);
		if (page == null) {
			return AudioReference.NO_TRACK;
		}

		var track = this.getPartnerTrackData(page);
		if (track.isNull()) {
			return AudioReference.NO_TRACK;
		}

		if (track.get("__typename").safeText().equals("Episode") || track.get("mediaType").safeText().equals("AUDIOBOOK_CHAPTER")) {
			return AudioReference.NO_TRACK;
		}

		if (!this.localFiles && track.get("uri").safeText().startsWith("spotify:local:")) {
			return AudioReference.NO_TRACK;
		}

		String metadataIsrc = null;
		var trackId = this.extractSpotifyId(track.get("uri").text());
		if (trackId != null && !trackId.isBlank() && this.tokenTracker.hasValidAccountCredentials()) {
			try {
				metadataIsrc = this.getTrackIsrcFromMetadata(trackId);
			} catch (Exception e) {
				log.debug("Failed to fetch ISRC from metadata for track {}", trackId, e);
			}
		}

		return this.parsePartnerTrack(track, preview, metadataIsrc);
	}

	private String getQueryValue(String query) {
		for (var part : query.split("&")) {
			var idx = part.indexOf('=');
			if (idx <= 0 || idx + 1 >= part.length()) {
				continue;
			}

			var currentKey = part.substring(0, idx);
			if (!currentKey.equals("seed_tracks")) {
				continue;
			}

			return URLDecoder.decode(part.substring(idx + 1), StandardCharsets.UTF_8);
		}

		return null;
	}

	private List<AudioTrack> parsePartnerTrackCollection(JsonBrowser items, boolean preview) {
		var tracks = new ArrayList<AudioTrack>();
		for (var value : items.values()) {
			var track = this.getPartnerTrackNode(value);
			if (track.isNull()) {
				continue;
			}

			if (track.get("__typename").safeText().equals("Episode") || track.get("mediaType").safeText().equals("AUDIOBOOK_CHAPTER")) {
				continue;
			}

			if (!this.localFiles && track.get("uri").safeText().startsWith("spotify:local:")) {
				continue;
			}

			tracks.add(this.parsePartnerTrack(track, preview, null));
		}

		return tracks;
	}

	private JsonBrowser getPartnerTrackNode(JsonBrowser value) {
		var node = this.firstNonNull(
			value.get("itemV2").get("data"),
			value.get("item").get("data"),
			value.get("track").get("data"),
			value.get("item"),
			value.get("track"),
			value.get("data"),
			value
		);

		if (!node.isNull() && node.get("__typename").safeText().endsWith("ResponseWrapper")) {
			var data = node.get("data");
			if (!data.isNull()) {
				return data;
			}
		}

		return node;
	}

	private JsonBrowser getPartnerDataNode(JsonBrowser value) {
		var node = this.firstNonNull(
			value.get("itemV2").get("data"),
			value.get("item").get("data"),
			value.get("item"),
			value.get("data"),
			value
		);

		if (!node.isNull() && node.get("__typename").safeText().endsWith("ResponseWrapper")) {
			var data = node.get("data");
			if (!data.isNull()) {
				return data;
			}
		}

		return node;
	}

	private JsonBrowser getPartnerPlaylistJson(String id, int offset, String market) throws IOException {
		var variables = "{\"uri\":\"spotify:playlist:" + id + "\",\"offset\":" + offset + ",\"limit\":" + SpotifySourceManager.PLAYLIST_MAX_PAGE_ITEMS + ",\"enableWatchFeedEntrypoint\":false,\"market\":\"from_token\",\"locale\":\"\",\"textFilter\":\"\",\"includeExtendedAudioItems\":false}";
		return this.getPartnerJson(PartnerQuery.GET_PLAYLIST, variables);
	}

	private JsonBrowser getPartnerAlbumJson(String id, int offset, String market) throws IOException {
		var variables = "{\"uri\":\"spotify:album:" + id + "\",\"offset\":" + offset + ",\"limit\":" + SpotifySourceManager.ALBUM_MAX_PAGE_ITEMS + ",\"market\":\"from_token\"}";
		return this.getPartnerJson(PartnerQuery.GET_ALBUM, variables);
	}

	private JsonBrowser getPartnerArtistJson(String id, String market) throws IOException {
		var variables = "{\"uri\":\"spotify:artist:" + id + "\",\"locale\":\"\",\"market\":\"from_token\"}";
		return this.getPartnerJson(PartnerQuery.GET_ARTIST, variables);
	}

	private JsonBrowser getPartnerRecommendationsJson(String trackId, String market) throws IOException {
		var variables = "{\"uri\":\"spotify:track:" + trackId + "\",\"market\":\"from_token\"}";
		return this.getPartnerJson(PartnerQuery.GET_RECOMMENDATIONS, variables);
	}

	private JsonBrowser getPartnerSearchJson(String query, int limit, String market) throws IOException {
		var escapedQuery = query.replace("\\", "\\\\").replace("\"", "\\\"");
		var variables = "{\"searchTerm\":\"" + escapedQuery + "\",\"offset\":" + 0 + ",\"limit\":" + limit + ",\"numberOfTopResults\":5,\"includeAudiobooks\":false,\"includeArtistHasConcertsField\":false,\"includeLocalConcertsField\":false,\"includePreReleases\":false,\"includeLocalMusic\":false,\"market\":\"from_token\"}";
		return this.getPartnerJson(PartnerQuery.SEARCH_DESKTOP, variables);
	}

	private JsonBrowser getPartnerTrackJson(String id, String market) throws IOException {
		var variables = "{\"uri\":\"spotify:track:" + id + "\",\"market\":\"from_token\"}";
		return this.getPartnerJson(PartnerQuery.GET_TRACK, variables);
	}

	private JsonBrowser getPartnerTrackData(JsonBrowser page) {
		var data = page.get("data");

		var track = data.get("trackUnion");
		if (!track.isNull()) {
			if (track.get("uri").text() != null) {
				return track;
			}

			var trackData = track.get("data");
			if (!trackData.isNull()) {
				return trackData;
			}
		}

		track = data.get("track");
		if (!track.isNull()) {
			if (track.get("uri").text() != null) {
				return track;
			}

			var trackData = track.get("data");
			if (!trackData.isNull()) {
				return trackData;
			}
		}

		track = data.get("entityV2").get("data");
		if (!track.isNull()) {
			return track;
		}

		track = data.get("itemV2").get("data");
		if (!track.isNull()) {
			return track;
		}

		return JsonBrowser.NULL_BROWSER;
	}

	private JsonBrowser getPartnerAlbumData(JsonBrowser page) {
		var data = page.get("data");

		var album = this.firstNonNull(
			data.get("albumUnion"),
			data.get("album"),
			data.get("entityV2").get("data"),
			data.get("itemV2").get("data")
		);

		if (!album.isNull() && album.get("data").isMap()) {
			return album.get("data");
		}

		return album;
	}

	private JsonBrowser getPartnerArtistData(JsonBrowser page) {
		var data = page.get("data");
		var artist = this.firstNonNull(
			data.get("artistUnion"),
			data.get("artist"),
			data.get("entityV2").get("data"),
			data.get("itemV2").get("data")
		);

		if (!artist.isNull() && artist.get("data").isMap()) {
			return artist.get("data");
		}

		return artist;
	}

	private JsonBrowser getPartnerSearchData(JsonBrowser page) {
		var data = page.get("data");
		return this.firstNonNull(
			data.get("searchV2"),
			data.get("searchDesktop"),
			data.get("search")
		);
	}

	private JsonBrowser getPartnerSearchItems(JsonBrowser searchData, String type) {
		switch (type) {
			case "tracks":
				return this.firstNonNull(searchData.get("tracksV2").get("items"), searchData.get("tracks").get("items"));
			case "albums":
				return this.firstNonNull(searchData.get("albumsV2").get("items"), searchData.get("albums").get("items"));
			case "artists":
				return this.firstNonNull(searchData.get("artistsV2").get("items"), searchData.get("artists").get("items"));
			case "playlists":
				return this.firstNonNull(searchData.get("playlistsV2").get("items"), searchData.get("playlists").get("items"));
			default:
				return JsonBrowser.NULL_BROWSER;
		}
	}

	private JsonBrowser firstNonNull(JsonBrowser... candidates) {
		for (var candidate : candidates) {
			if (!candidate.isNull()) {
				return candidate;
			}
		}

		return JsonBrowser.NULL_BROWSER;
	}

	private JsonBrowser getPartnerJson(PartnerQuery query, String variablesJson) throws IOException {
		var request = new HttpPost(PARTNER_API_BASE);
		request.setHeader("User-Agent", USER_AGENT);
		request.setHeader("Authorization", "Bearer " + this.tokenTracker.getAnonymousAccessToken());
		request.setHeader("App-Platform", "WebPlayer");
		var payload = "{\"operationName\":\"" + query.operationName + "\",\"variables\":" + variablesJson + ",\"extensions\":{\"persistedQuery\":{\"version\":1,\"sha256Hash\":\"" + query.sha256Hash + "\"}}}";
		request.setEntity(new StringEntity(payload, ContentType.APPLICATION_JSON));
		return LavaSrcTools.fetchResponseAsJson(this.httpInterfaceManager.getInterface(), request);
	}

	private AudioTrack parsePartnerTrack(JsonBrowser json, boolean preview) {
		return this.parsePartnerTrack(json, preview, null);
	}

	private AudioTrack parsePartnerTrack(JsonBrowser json, boolean preview, String fallbackIsrc) {
		var uri = json.get("uri").text();
		var trackId = this.extractSpotifyId(uri);
		if (trackId == null || trackId.isBlank()) {
			trackId = "local";
		}

		var artists = json.get("artists").get("items").values();
		if (artists.isEmpty()) {
			artists = json.get("firstArtist").get("items").values();
		}

		JsonBrowser firstArtist = artists.isEmpty() ? null : artists.get(0);
		var artistName = firstArtist == null ? "" : firstArtist.get("profile").get("name").safeText();
		if (artistName.isEmpty()) {
			artistName = "Unknown";
		}

		var artistUri = firstArtist == null ? null : firstArtist.get("uri").text();
		var artistUrl = this.toOpenSpotifyUrl(artistUri);
		var artistArtwork = firstArtist == null ? null : firstArtist.get("visuals").get("avatarImage").get("sources").index(0).get("url").text();

		var album = json.get("albumOfTrack");
		var albumName = album.get("name").text();
		var albumUrl = this.toOpenSpotifyUrl(album.get("uri").text());
		var albumArtworkUrl = album.get("coverArt").get("sources").index(0).get("url").text();

		var duration = json.get("trackDuration").get("totalMilliseconds").asLong(0);
		if (duration == 0) {
			duration = json.get("duration").get("totalMilliseconds").asLong(0);
		}

		String isrc = null;
		for (var externalId : json.get("externalIds").get("items").values()) {
			if (externalId.get("type").safeText().equalsIgnoreCase("isrc")) {
				isrc = externalId.get("id").text();
				if (isrc == null || isrc.isBlank()) {
					isrc = externalId.get("value").text();
				}
				break;
			}
		}

		if ((isrc == null || isrc.isBlank()) && fallbackIsrc != null && !fallbackIsrc.isBlank()) {
			isrc = fallbackIsrc;
		}

		var trackUrl = json.get("shareUrl").text();
		if (trackUrl == null || trackUrl.isBlank()) {
			trackUrl = this.toOpenSpotifyUrl(uri);
		}

		var previewUrl = json.get("audioPreview").get("url").text();

		return new SpotifyAudioTrack(
			new AudioTrackInfo(
				json.get("name").safeText(),
				artistName,
				preview ? PREVIEW_LENGTH : duration,
				trackId,
				false,
				trackUrl,
				albumArtworkUrl,
				isrc
			),
			albumName,
			albumUrl,
			artistUrl,
			artistArtwork,
			previewUrl,
			preview,
			this
		);
	}

	private String getTrackIsrcFromMetadata(String trackId) throws IOException {
		var gid = this.toSpotifyGid(trackId);
		if (gid == null) {
			return null;
		}

		var request = new HttpGet(CLIENT_API_BASE + "metadata/4/track/" + gid + "?market=from_token");
		request.setHeader("Authorization", "Bearer " + this.tokenTracker.getAccountAccessToken());
		request.setHeader("Accept", "application/json");
		request.setHeader("Origin", "open.spotify.com");
		request.setHeader("Referer", "open.spotify.com");

		var metadata = LavaSrcTools.fetchResponseAsJson(this.httpInterfaceManager.getInterface(), request);
		if (metadata == null) {
			return null;
		}

		return this.extractIsrcFromMetadata(metadata);
	}

	private String extractIsrcFromMetadata(JsonBrowser metadata) {
		var direct = this.firstNonBlank(
			metadata.get("isrc").text(),
			metadata.get("external_id").get("isrc").text(),
			metadata.get("externalId").get("isrc").text(),
			metadata.get("externalIds").get("isrc").text(),
			metadata.get("track").get("isrc").text(),
			metadata.get("track").get("external_id").get("isrc").text(),
			metadata.get("track").get("externalId").get("isrc").text(),
			metadata.get("track").get("externalIds").get("isrc").text()
		);

		if (direct != null) {
			return direct;
		}

		var externalIds = this.firstNonNull(
			metadata.get("external_id"),
			metadata.get("external_ids"),
			metadata.get("externalIds"),
			metadata.get("external_id").get("items"),
			metadata.get("externalIds").get("items"),
			metadata.get("track").get("external_id"),
			metadata.get("track").get("external_ids"),
			metadata.get("track").get("externalIds"),
			metadata.get("track").get("external_id").get("items"),
			metadata.get("track").get("externalIds").get("items")
		);

		if (!externalIds.isNull()) {
			for (var item : externalIds.values()) {
				var type = this.firstNonBlank(item.get("type").text(), item.get("name").text(), item.get("key").text());
				if (type != null && type.equalsIgnoreCase("isrc")) {
					return this.firstNonBlank(item.get("id").text(), item.get("value").text(), item.get("code").text());
				}
			}
		}

		return null;
	}

	private String toSpotifyGid(String base62Id) {
		if (base62Id == null || base62Id.isBlank()) {
			return null;
		}

		final String alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
		BigInteger value = BigInteger.ZERO;
		for (var c : base62Id.toCharArray()) {
			var index = alphabet.indexOf(c);
			if (index < 0) {
				return null;
			}
			value = value.multiply(BigInteger.valueOf(62)).add(BigInteger.valueOf(index));
		}

		return String.format("%032x", value);
	}

	private String firstNonBlank(String... values) {
		for (var value : values) {
			if (value != null && !value.isBlank()) {
				return value;
			}
		}

		return null;
	}

	private String extractSpotifyId(String uri) {
		if (uri == null || uri.isBlank()) {
			return null;
		}

		var parts = uri.split(":");
		if (parts.length < 3) {
			return null;
		}

		if (!parts[0].equals("spotify")) {
			return null;
		}

		if (!parts[1].equals("track")) {
			return null;
		}

		return parts[2];
	}

	private String toOpenSpotifyUrl(String uri) {
		if (uri == null || uri.isBlank()) {
			return null;
		}

		if (uri.startsWith("http://") || uri.startsWith("https://")) {
			return uri;
		}

		var parts = uri.split(":");
		if (parts.length < 3 || !parts[0].equals("spotify")) {
			return null;
		}

		return "https://open.spotify.com/" + parts[1] + "/" + parts[2];
	}

	@Override
	public void shutdown() {
		try {
			this.httpInterfaceManager.close();
		} catch (IOException e) {
			log.error("Failed to close HTTP interface manager", e);
		}
	}

	@Override
	public void configureRequests(Function<RequestConfig, RequestConfig> configurator) {
		this.httpInterfaceManager.configureRequests(configurator);
	}

	@Override
	public void configureBuilder(Consumer<HttpClientBuilder> configurator) {
		this.httpInterfaceManager.configureBuilder(configurator);
	}

	private enum PartnerQuery {
		GET_TRACK("getTrack", "612585ae06ba435ad26369870deaae23b5c8800a256cd8a57e08eddc25a37294"),
		GET_ALBUM("getAlbum", "b9bfabef66ed756e5e13f68a942deb60bd4125ec1f1be8cc42769dc0259b4b10"),
		GET_PLAYLIST("fetchPlaylist", "bb67e0af06e8d6f52b531f97468ee4acd44cd0f82b988e15c2ea47b1148efc77"),
		GET_ARTIST("queryArtistOverview", "35648a112beb1794e39ab931365f6ae4a8d45e65396d641eeda94e4003d41497"),
		GET_RECOMMENDATIONS("internalLinkRecommenderTrack", "c77098ee9d6ee8ad3eb844938722db60570d040b49f41f5ec6e7be9160a7c86b"),
		SEARCH_DESKTOP("searchDesktop", "fcad5a3e0d5af727fb76966f06971c19cfa2275e6ff7671196753e008611873c");

		private final String operationName;
		private final String sha256Hash;

		PartnerQuery(String operationName, String sha256Hash) {
			this.operationName = operationName;
			this.sha256Hash = sha256Hash;
		}
	}
}