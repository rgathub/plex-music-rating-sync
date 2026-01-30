import abc
import logging
import getpass
import plexapi.playlist
import plexapi.audio
from plexapi.exceptions import BadRequest, NotFound
from plexapi.myplex import MyPlexAccount
import time
from typing import List, Optional, Union

from sync_items import AudioTag, Playlist


class MediaPlayer(abc.ABC):
	album_empty_alias = ''
	dry_run = False
	reverse = False
	rating_maximum = 5

	@staticmethod
	@abc.abstractmethod
	def name():
		"""
		The name of this media player
		:return: name of this media player
		:rtype: str
		"""
		return ''

	@staticmethod
	@abc.abstractclassmethod
	def format(track):
		# TODO maybe makes more sense to create a track class and make utility functions for __str__, artist, album, title, etc
		# but having to know what player you are working with up front wasn't workable
		"""
		Returns a formatted representation of a track in the format of
		artist name - album name - track title
		"""
		return NotImplementedError

	def album_empty(self, album):
		if not isinstance(album, str):
			return False
		return album == self.album_empty_alias

	def connect(self, *args, **kwargs):
		return NotImplemented

	@abc.abstractmethod
	def create_playlist(self, title: str, tracks: List[object]):
		"""
		Creates a playlist unless in dry run
		"""

	@staticmethod
	def get_5star_rating(rating):
		return rating * 5

	def get_native_rating(self, normed_rating):
		return normed_rating * self.rating_maximum

	def get_normed_rating(self, rating: Optional[float]):
		if (rating or 0) <= 0:
			rating = 0
		return rating / self.rating_maximum

	@abc.abstractmethod
	def read_playlists(self):
		"""

		:return: a list of all playlists that exist, including automatically generated playlists
		:rtype: list<Playlist>
		"""

	@abc.abstractmethod
	def read_track_metadata(self, track) -> AudioTag:
		"""

		:param track: The track for which to read the metadata.
		:return: The metadata stored in an audio tag instance.
		"""

	@abc.abstractmethod
	def find_playlist(self, **nargs):
		"""

		:param nargs:
		:return: a list of playlists matching the search parameters
		:rtype: list<Playlist>
		"""

	@abc.abstractmethod
	def search_tracks(self, key: str, value: Union[bool, str]) -> List[AudioTag]:
		"""Search the MediaMonkey music library for tracks matching the artist and track title.

		:param key: The search mode. Valid modes are:

			* *rating*  -- Search for tracks that have a rating.
			* *title*   -- Search by track title.
			* *query*   -- MediaMonkey query string, free form.

		:param value: The value to search for.

		:return: a list of matching tracks
		:rtype: list<sync_items.AudioTag>
		"""
		pass

	@abc.abstractmethod
	def update_playlist(self, playlist, track, present: bool):
		"""Updates the playlist, unless in dry run
		:param playlist:
			The playlist native to this player that shall be updated
		:param track:
			The track to update
		:param present:
		"""

	@abc.abstractmethod
	def update_rating(self, track, rating):
		"""Updates the rating of the track, unless in dry run"""

	def __hash__(self):
		return hash(self.name().lower())

	def __eq__(self, other):
		if not isinstance(other, type(self)):
			return NotImplemented
		return other.name().lower() == self.name().lower()


class MediaMonkey(MediaPlayer):
	rating_maximum = 100

	def __init__(self):
		super(MediaMonkey, self).__init__()
		self.logger = logging.getLogger('PlexSync.MediaMonkey')
		self.conn = None
		self.cursor = None
		self.db_path = None

	@staticmethod
	def name():
		return 'MediaMonkey'

	@staticmethod
	def format(track):
		# TODO maybe makes more sense to create a track class and make utility functions for __str__, artist, album, title, etc
		return ' - '.join([track.artist, track.album, track.title])

	def connect(self, db_path=None, *args, **kwargs):
		"""
		Connect to MediaMonkey database
		:param db_path: Path to MM.DB file. If None, attempts to auto-detect.
		"""
		self.logger.info('Connecting to local player {} database'.format(self.name()))
		import sqlite3
		import os
		
		# Auto-detect database location if not provided
		if db_path is None:
			# Try MediaMonkey 5.x location
			appdata = os.getenv('APPDATA')
			if appdata:
				mm5_path = os.path.join(appdata, 'MediaMonkey5', 'MM5.DB')
				if os.path.exists(mm5_path):
					db_path = mm5_path
					self.logger.info('Found MediaMonkey 5.x database at: {}'.format(db_path))
			
			# Try MediaMonkey 4.x location if 5.x not found
			if db_path is None:
				if appdata:
					mm4_path = os.path.join(appdata, 'MediaMonkey', 'MM.DB')
					if os.path.exists(mm4_path):
						db_path = mm4_path
						self.logger.info('Found MediaMonkey 4.x database at: {}'.format(db_path))
				
		
		if db_path is None or not os.path.exists(db_path):
			self.logger.error('MediaMonkey database not found. Please specify the path with --db-path.')
			self.logger.error('Typical locations:')
			self.logger.error('  MediaMonkey 4.x: %APPDATA%\\MediaMonkey\\MM.DB')
			self.logger.error('  MediaMonkey 5.x: %LOCALAPPDATA%\\MediaMonkey\\DB\\MM.DB')
			exit(1)
		
		try:
			self.db_path = db_path
			# Open in read-only mode to prevent corruption and allow concurrent access
			uri = 'file:{}?mode=ro'.format(db_path.replace('\\', '/'))
			self.conn = sqlite3.connect(uri, uri=True)
			self.conn.row_factory = sqlite3.Row  # Access columns by name
			self.cursor = self.conn.cursor()
			self.logger.info('Successfully connected to MediaMonkey database')
			
			# Verify database structure
			self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Songs'")
			if not self.cursor.fetchone():
				self.logger.error('Invalid MediaMonkey database: Songs table not found')
				exit(1)
		except Exception as e:
			self.logger.error('Failed to connect to MediaMonkey database: {}'.format(e))
			exit(1)

	def create_playlist(self, title, tracks):
		raise NotImplementedError

	def find_playlist(self, **nargs):
		raise NotImplementedError

	def read_child_playlists(self, parent_id, parent_name=''):
		"""
		Recursively read child playlists from database
		:param parent_id: Parent playlist ID (-1 for root)
		:param parent_name: Parent playlist name for hierarchy
		:rtype: list<Playlist>
		"""
		playlists = []
		
		# Query child playlists
		query = """
			SELECT IDPlaylist, PlaylistName, isAutoPlaylist
			FROM Playlists
			WHERE ParentPlaylist = ?
			ORDER BY PlaylistName
		"""
		self.cursor.execute(query, (parent_id,))
		
		for row in self.cursor.fetchall():
			playlist_id = row['IDPlaylist']
			playlist_name = row['PlaylistName']
			is_auto = bool(row['isAutoPlaylist'])
			
			playlist = Playlist(playlist_name, parent_name=parent_name)
			playlist.is_auto_playlist = is_auto
			playlists.append(playlist)
			
			if is_auto:
				self.logger.debug('Skipping to read tracks for auto playlist {}'.format(playlist.name))
			else:
				# Read tracks for this playlist
				track_query = """
					SELECT s.ID, s.SongTitle, s.Artist, s.Album, s.TrackNumber, s.Rating, s.SongPath
					FROM Songs s
					INNER JOIN PlaylistSongs ps ON s.ID = ps.IDSong
					WHERE ps.IDPlaylist = ?
					ORDER BY ps.SongOrder
				"""
				self.cursor.execute(track_query, (playlist_id,))
				for track_row in self.cursor.fetchall():
					playlist.tracks.append(self._row_to_audiotag(track_row))
			
			# Recursively read child playlists
			child_playlists = self.read_child_playlists(playlist_id, playlist.name)
			playlists.extend(child_playlists)
		
		return playlists

	def read_playlists(self):
		self.logger.info('Reading playlists from the {} player'.format(self.name()))
		# Start with root playlists (ParentPlaylist = -1)
		playlists = self.read_child_playlists(-1)
		self.logger.info('Found {} playlists'.format(len(playlists)))
		return playlists

	def _row_to_audiotag(self, row) -> AudioTag:
		"""
		Convert database row to AudioTag object
		:param row: sqlite3.Row object
		:return: AudioTag instance
		"""
		artist = row['Artist'] or ''
		album = row['Album'] or ''
		title = row['SongTitle'] or ''
		file_path = row['SongPath'] or ''
		
		tag = AudioTag(artist=artist, album=album, title=title, file_path=file_path)
		tag.rating = self.get_normed_rating(row['Rating'])
		tag.ID = row['ID']
		tag.track = row['TrackNumber'] or 0
		return tag

	def read_track_metadata(self, track) -> AudioTag:
		"""
		Read track metadata from database by ID
		:param track: AudioTag with ID set, or integer ID
		:return: AudioTag with full metadata
		"""
		track_id = track.ID if isinstance(track, AudioTag) else track
		
		query = """
			SELECT ID, SongTitle, Artist, Album, TrackNumber, Rating, SongPath
			FROM Songs
			WHERE ID = ?
		"""
		self.cursor.execute(query, (track_id,))
		row = self.cursor.fetchone()
		
		if row:
			return self._row_to_audiotag(row)
		else:
			self.logger.warning('Track with ID {} not found in database'.format(track_id))
			return None

	def search_tracks(self, key: str, value: Union[bool, str]) -> List[AudioTag]:
		"""
		Search for tracks in MediaMonkey database
		:param key: Search mode ('title', 'rating', 'query')
		:param value: Search value
		:return: List of matching AudioTag objects
		"""
		if not value:
			raise ValueError(f"value can not be empty.")
		
		tags = []
		
		if key == "title":
			# Search by exact title
			query = """
				SELECT ID, SongTitle, Artist, Album, TrackNumber, Rating, SongPath
				FROM Songs
				WHERE SongTitle = ?
			"""
			self.logger.debug(f'Searching for tracks with title: {value}')
			self.cursor.execute(query, (value,))
			
		elif key == "rating":
			# Search by rating
			if value is True:
				# Get all rated tracks
				query = """
					SELECT ID, SongTitle, Artist, Album, TrackNumber, Rating, SongPath
					FROM Songs
					WHERE Rating > 0
				"""
				self.logger.info('Reading tracks from the {} player'.format(self.name()))
				self.cursor.execute(query)
			else:
				# Custom rating condition (e.g., "> 50", "= 100")
				query = f"""
					SELECT ID, SongTitle, Artist, Album, TrackNumber, Rating, SongPath
					FROM Songs
					WHERE Rating {value}
				"""
				self.logger.debug(f'Executing rating query: Rating {value}')
				self.cursor.execute(query)
				
		elif key == "query":
			# Direct SQL query (advanced usage)
			# Wrap in SELECT from Songs if not already a complete query
			if not value.strip().upper().startswith('SELECT'):
				query = f"""
					SELECT ID, SongTitle, Artist, Album, TrackNumber, Rating, SongPath
					FROM Songs
					WHERE {value}
				"""
			else:
				query = value
			self.logger.debug(f'Executing custom query: {query}')
			self.cursor.execute(query)
			
		else:
			raise KeyError(f"Invalid search mode {key}.")
		
		# Fetch results and convert to AudioTag objects
		for row in self.cursor.fetchall():
			tags.append(self._row_to_audiotag(row))
		
		self.logger.info(f'Found {len(tags)} tracks.')
		return tags

	def update_playlist(self, playlist, track, present):
		raise NotImplementedError

	def update_rating(self, track, rating):
		"""
		Update track rating in MediaMonkey database
		Note: Requires write access - database should not be opened in read-only mode
		:param track: AudioTag with ID set
		:param rating: Normalized rating (0-1)
		"""
		self.logger.debug('Updating rating of track "{}" to {} stars'.format(
			self.format(track), self.get_5star_rating(rating))
		)
		if not self.dry_run:
			# Check if database is read-only
			if 'mode=ro' in str(self.db_path):
				self.logger.error('Cannot update ratings: database opened in read-only mode')
				self.logger.error('Restart with write access or close MediaMonkey')
				return
			
			try:
				# Reopen connection with write access if needed
				if self.conn and not self.conn.execute("PRAGMA query_only").fetchone()[0] == 0:
					self.conn.close()
					self.conn = sqlite3.connect(self.db_path)
					self.conn.row_factory = sqlite3.Row
					self.cursor = self.conn.cursor()
				
				native_rating = self.get_native_rating(rating)
				query = """
					UPDATE Songs
					SET Rating = ?
					WHERE ID = ?
				"""
				self.cursor.execute(query, (native_rating, track.ID))
				self.conn.commit()
				self.logger.debug('Successfully updated rating for track ID {}'.format(track.ID))
				
			except sqlite3.OperationalError as e:
				self.logger.error('Failed to update rating: {} (Is MediaMonkey running?)'.format(e))
			except Exception as e:
				self.logger.error('Unexpected error updating rating: {}'.format(e))
				self.conn.rollback()
	
	def __del__(self):
		"""Close database connection on cleanup"""
		if hasattr(self, 'conn') and self.conn:
			self.conn.close()


class PlexPlayer(MediaPlayer):
	# TODO logging needs to be updated to reflect whether Plex is source or destination
	maximum_connection_attempts = 3
	rating_maximum = 10
	album_empty_alias = '[Unknown Album]'

	def __init__(self):
		super(PlexPlayer, self).__init__()
		self.logger = logging.getLogger('PlexSync.PlexPlayer')
		self.account = None
		self.plex_api_connection = None
		self.music_library = None

	@staticmethod
	def name():
		return 'PlexPlayer'

	@staticmethod
	def format(track):
		# TODO maybe makes more sense to create a track class and make utility functions for __str__, artist, album, title, etc
		try:
			return ' - '.join([track.artist().title, track.album().title, track.title])
		except TypeError:
			return ' - '.join([track.artist, track.album, track.title])

	def connect(self, server, username, password='', token=''):
		self.logger.info(f'Connecting to the Plex Server {server} with username {username}.')
		connection_attempts_left = self.maximum_connection_attempts
		while connection_attempts_left > 0:
			time.sleep(1)  # important. Otherwise, the above print statement can be flushed after
			if (not password) & (not token):
				password = getpass.getpass()
			try:
				if (password):
					self.account = MyPlexAccount(username=username, password=password)
				elif (token):
					self.account = MyPlexAccount(username=username, token=token)
				break
			except NotFound:
				print(f'Username {username}, password or token wrong for server {server}.')
				password = ''
				connection_attempts_left -= 1
			except BadRequest as error:
				self.logger.warning('Failed to connect: {}'.format(error))
				connection_attempts_left -= 1
		if connection_attempts_left == 0 or self.account is None:
			self.logger.error('Exiting after {} failed attempts ...'.format(self.maximum_connection_attempts))
			exit(1)

		self.logger.info('Connecting to remote player {} on the server {}'.format(self.name(), server))
		try:
			self.plex_api_connection = self.account.resource(server).connect(timeout=5)
			self.logger.info('Successfully connected')
		except NotFound:
			# This also happens if the user is not the owner of the server
			self.logger.error('Error: Unable to connect')
			exit(1)

		self.logger.info('Looking for music libraries')
		music_libraries = {
			section.key:
				section for section
				in self.plex_api_connection.library.sections()
				if section.type == 'artist'}

		if len(music_libraries) == 0:
			self.logger.error('No music library found')
			exit(1)
		elif len(music_libraries) == 1:
			self.music_library = list(music_libraries.values())[0]
			self.logger.debug('Found 1 music library')
		else:
			print('Found multiple music libraries:')
			for key, library in music_libraries.items():
				print('\t[{}]: {}'.format(key, library.title))

			choice = input('Select the library to sync with: ')
			self.music_library = music_libraries[int(choice)]

	def read_track_metadata(self, track: plexapi.audio.Track) -> AudioTag:
		tag = AudioTag(artist=track.grandparentTitle, album=track.parentTitle, title=track.title, file_path=track.locations[0])
		tag.rating = self.get_normed_rating(track.userRating)
		tag.track = track.index
		tag.ID = track.key
		return tag

	def create_playlist(self, title, tracks: List[plexapi.audio.Track]) -> Optional[plexapi.playlist.Playlist]:
		self.logger.info('Creating playlist {} on the server'.format(title))
		if self.dry_run:
			return None
		else:
			if tracks is None or len(tracks) == 0:
				self.logger.warning('Playlist {} can not be created without supplying at least one track. Skipping.'.format(title))
				return None
			return self.plex_api_connection.createPlaylist(title=title, items=tracks)

	def read_playlists(self):
		raise NotImplementedError

	def find_playlist(self, **kwargs) -> Optional[plexapi.playlist.Playlist]:
		"""

		:param kwargs:
			See below

		:keyword Arguments:
			* *title* (``str``) -- Playlist name

		:return: a list of matching playlists
		:rtype: list<Playlist>
		"""
		title = kwargs['title']
		try:
			return self.plex_api_connection.playlist(title)
		except NotFound:
			self.logger.debug('Playlist {} not found on the remote player'.format(title))
			return None

	def search_tracks(self, key: str, value: Union[bool, str]) -> List[AudioTag]:
		if not value:
			raise ValueError(f"value can not be empty.")
		if key == "title":
			matches = self.music_library.searchTracks(title=value)
			n_matches = len(matches)
			s_matches = f"match{'es' if n_matches > 1 else ''}"
			self.logger.debug(f'Found {n_matches} {s_matches} for query title={value}')
		elif key == "rating":
			if value is True:
				value = "0"
			matches = self.music_library.searchTracks(**{'track.userRating!': value})
			tags = []
			counter = 0
			for x in matches:
				tags.append(self.read_track_metadata(x))
				counter += 1
			self.logger.info('Found {} tracks with a rating > 0 that need syncing'.format(counter))
			matches = tags
		else:
			raise KeyError(f"Invalid search mode {key}.")
		return matches

	def update_playlist(self, playlist, track, present):
		"""
		:type playlist: plexapi.playlist.Playlist
		:type track: plexapi.audio.Track
		:type present: bool
		:return:
		"""
		if present:
			self.logger.debug('Adding {} to playlist {}'.format(self.format(track), playlist.title))
			if not self.dry_run:
				playlist.addItems(track)
		else:
			self.logger.debug('Removing {} from playlist {}'.format(self.format(track), playlist.title))
			if not self.dry_run:
				playlist.removeItem(track)

	def update_rating(self, track, rating):
		self.logger.debug('Updating rating of track "{}" to {} stars'.format(
			self.format(track), self.get_5star_rating(rating))
		)
		if not self.dry_run:
			try:
				track.edit(**{'userRating.value': self.get_native_rating(rating)})
			except AttributeError:
				song = [s for s in self.music_library.searchTracks(title=track.title) if s.key == track.ID][0]
				song.edit(**{'userRating.value': self.get_native_rating(rating)})
