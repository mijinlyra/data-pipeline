// Authorization token that must have been created previously. See : https://developer.spotify.com/documentation/web-api/concepts/authorization
const token = 'BQDm0LQd5tnzWt-vgOuOp2_-fvid2dDzNkee_W0_s4j-kmbBN_lfmc6cDhGKiuYJOQx6_8Yu8gYEdcYttd2SFzbfphvuWSC6xF0jlVMxQOjPQYHjfJW-ps7AeUIgYj-r5RWims2ywzKzs1d7p07-xNmQsoJ_WdGNSkjfk21xEu72LrghZmzaWrbLH6ua3A4YOINTKD9lywXEvif0q_XolqEsCYdX_oAWpXaVY-idIC-bQk6PyU7xqkXUfL8r_OfC_U0r3_F2vJQpWpQ5FRGLqiz-TblF';
async function fetchWebApi(endpoint, method, body) {
  const res = await fetch(`https://api.spotify.com/${endpoint}`, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
    method,
    body:JSON.stringify(body)
  });
  return await res.json();
}

async function getTopTracks(){
  // Endpoint reference : https://developer.spotify.com/documentation/web-api/reference/get-users-top-artists-and-tracks
  return (await fetchWebApi(
    'v1/me/top/tracks?time_range=short_term&limit=5', 'GET'
  )).items;
}

const topTracks = await getTopTracks();
console.log(
  topTracks?.map(
    ({name, artists}) =>
      `${name} by ${artists.map(artist => artist.name).join(', ')}`
  )
);
